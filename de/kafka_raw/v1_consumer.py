"""Single-owner V1 worker. Kafka commits follow confirmed transport persistence."""
import logging
import time
import uuid
from collections import defaultdict
from confluent_kafka import Consumer, TopicPartition, TopicCollection, KafkaException
from confluent_kafka.admin import AdminClient
from de.kafka_raw.v1_models import V1Record
from de.kafka_raw.v1_validator import V1RawValidator
from de.kafka_raw.v1_ledger_store import IntegrityFault

log=logging.getLogger(__name__)

class V1RawKafkaConsumer:
    def __init__(self, settings, repo, ledger):
        self.settings,self.repo,self.ledger=settings,repo,ledger
        self.validator=V1RawValidator()
        self.pending=[]
        self.assigned=set()
        self.state="STARTING"
        self.failure_reason=None
        self.running=True
        self.last_flush=time.monotonic()
        self.identity=None
        self.offset_snapshot=[]
        self.last_sample=0.0
        self.last_identity_check=0.0
        self.broker_ok=False
        self.counts=dict(received=0,persisted=0,retries=0)
        self.last_persistence=None
        self.last_commit=None
        self.consumer=Consumer({"bootstrap.servers":settings.bootstrap_servers,
            "group.id":settings.group_id,"group.instance.id":"v1-raw-local-single-owner",
            "enable.auto.commit":False,"enable.auto.offset.store":False,
            "auto.offset.reset":"error","isolation.level":"read_committed",
            "partition.assignment.strategy":"range","session.timeout.ms":45000,
            "queued.max.messages.kbytes":32768,
            "max.poll.interval.ms":300000})

    def start(self):
        if not self.repo.verify_tables():
            raise IntegrityFault("V1 migration schema is missing or incompatible")
        if self.ledger.db.execute("SELECT count(*) FROM integrity_faults").fetchone()[0]:
            raise IntegrityFault("unresolved durable integrity fault requires operator reconciliation")
        self.admin=AdminClient({"bootstrap.servers":self.settings.bootstrap_servers})
        for attempt in range(10):
            try:
                description=self.admin.describe_topics(TopicCollection([self.settings.topic]),
                                              request_timeout=5)[self.settings.topic].result()
                break
            except KafkaException:
                if attempt==9:
                    raise
                time.sleep(.5)
        self.identity=str(description.topic_id)
        if not self.identity or self.identity=="AAAAAAAAAAAAAAAAAAAAAA":
            raise IntegrityFault("actual topic UUID unavailable; refuse identity-space reuse")
        self.consumer.subscribe([self.settings.topic],on_assign=self.on_assign,
                                on_revoke=self.on_revoke,on_lost=self.on_lost)
        self.broker_ok=True

    def on_assign(self, consumer, partitions):
        try:
            self._assign_verified(consumer,partitions)
        except Exception as exc:
            self.assigned.clear()
            raise IntegrityFault("startup assignment reconciliation failed: "+str(exc)) from exc

    def _assign_verified(self, consumer, partitions):
        self.state="ASSIGNING"
        for tp in partitions:
            low,high=consumer.get_watermark_offsets(tp,timeout=10,cached=False)
            committed=consumer.committed([tp],timeout=10)[0].offset
            old=self.ledger.db.execute("SELECT * FROM partition_progress WHERE topic=? AND partition_id=?",
                                      (tp.topic,tp.partition)).fetchone()
            if old and old["topic_identity"]!=self.identity:
                raise IntegrityFault("topic UUID changed")
            if committed<0:
                if low>0 or (old and old["committed_next_offset"] not in (None, 0)):
                    raise IntegrityFault("missing broker commit or expired history")
                committed=low
            if committed<low or committed>high:
                raise IntegrityFault("RETENTION_GAP or out-of-range committed offset")
            if not old and committed>low:
                raise IntegrityFault("broker committed history has no local confirmation")
            if old:
                rows=self.ledger.db.execute("""SELECT * FROM deliveries
WHERE topic=? AND partition_id=? AND offset_value<?""",(tp.topic,tp.partition,committed)).fetchall()
                # Reconciliation compares transport evidence, independent of consumed_at representation.
                found=self.repo.evidence([r["raw_ingestion_id"] for r in rows])
                for r in rows:
                    matches=found[r["raw_ingestion_id"]]
                    if not matches or any(x["physical_record_hash"]!=r["physical_record_hash"] or
                                          x["destination"]!=r["destination"] for x in matches):
                        raise IntegrityFault("BROKER_COMMITTED_UNRECONCILED")
                safe=old["safe_next_offset"]
                if safe is None or committed>safe:
                    raise IntegrityFault("broker commit ahead of persisted safe boundary")
                self.ledger.transition([r["raw_ingestion_id"] for r in rows],"COMMITTED")
            self.ledger.db.execute("""INSERT INTO partition_progress VALUES(?,?,?,?,?)
ON CONFLICT(topic,partition_id) DO UPDATE SET committed_next_offset=excluded.committed_next_offset""",
                                  (tp.topic,tp.partition,self.identity,committed,committed))
            tp.offset=committed
            self.assigned.add((tp.topic,tp.partition))
        consumer.assign(partitions)
        self.state="READY"

    def on_revoke(self,consumer,partitions):
        try:
            self.flush()
        finally:
            self.assigned.difference_update((p.topic,p.partition) for p in partitions)
            self.pending=[c for c in self.pending if (c.row["topic"],c.row["partition"]) in self.assigned]

    def on_lost(self,consumer,partitions):
        self.assigned.difference_update((p.topic,p.partition) for p in partitions)
        self.pending=[c for c in self.pending if (c.row["topic"],c.row["partition"]) in self.assigned]

    def flush(self):
        if not self.pending: return
        records=list(self.pending)
        ids=[r.row["raw_ingestion_id"] for r in records]
        self.ledger.receive_batch(records,uuid.uuid4().hex)
        self.ledger.transition(ids,"PERSISTENCE_ATTEMPTED")
        existing=self.repo.reconcile(records)
        known={r.row["raw_ingestion_id"] for r in existing}
        missing=[r for r in records if r.row["raw_ingestion_id"] not in known]
        try:
            self.repo.insert(missing)
        except Exception:
            self.counts["retries"]+=1
            log.exception("ambiguous V1 insert; reconciling")
        confirmed=self.repo.reconcile(records)
        good={r.row["raw_ingestion_id"] for r in confirmed}
        self.ledger.transition(list(good),"PERSISTENCE_CONFIRMED")
        self.last_persistence=time.time()
        # Iterate actual delivered order, not integers: Kafka may omit aborted/control offsets.
        by_partition=defaultdict(list)
        for r in records:
            by_partition[(r.row["topic"],r.row["partition"])].append(r)
        for key,rows in by_partition.items():
            prefix=[]
            for r in rows:
                if r.row["raw_ingestion_id"] not in good: break
                prefix.append(r)
            if not prefix or key not in self.assigned: continue
            next_offset=prefix[-1].row["offset"]+1
            prefix_ids=[r.row["raw_ingestion_id"] for r in prefix]
            self.ledger.transition(prefix_ids,"COMMIT_ELIGIBLE")
            self.ledger.db.execute("UPDATE partition_progress SET safe_next_offset=? WHERE topic=? AND partition_id=?",
                                  (next_offset,*key))
            response=self.consumer.commit(offsets=[TopicPartition(*key,next_offset)],asynchronous=False)
            if any(p.error for p in response):
                raise RuntimeError("partition commit failed")
            self.ledger.transition(prefix_ids,"COMMITTED")
            self.last_commit=time.time()
            self.ledger.db.execute("UPDATE partition_progress SET committed_next_offset=? WHERE topic=? AND partition_id=?",
                                  (next_offset,*key))
            removed=set(prefix_ids)
            self.pending=[r for r in self.pending if r.row["raw_ingestion_id"] not in removed]
            self.counts["persisted"]+=len(prefix)
        if self.pending:
            raise RuntimeError("unresolved persistence")
        self.last_flush=time.monotonic()

    def _handle_msg(self, message):
        if message.error():
            raise RuntimeError(str(message.error()))
        if (message.topic(),message.partition()) not in self.assigned:
            raise IntegrityFault("record received without partition ownership")
        kind,stamp=message.timestamp()
        record=V1Record(message.topic(),message.partition(),message.offset(),
                        message.key(),message.value(),tuple(message.headers() or ()),
                        stamp if stamp>=0 else None,kind,self.identity)
        self.pending.append(self.validator.classify(record))
        self.counts["received"]+=1

    def run(self):
        try:
            self.start()
            while self.running:
                try:
                    if time.monotonic()-self.last_identity_check>30:
                        self.broker_ok=False
                        topic=self.admin.describe_topics(TopicCollection([self.settings.topic]),request_timeout=5)[self.settings.topic].result()
                        if str(topic.topic_id)!=self.identity:
                            raise IntegrityFault("topic UUID changed during ingestion")
                        self.broker_ok=True
                        self.last_identity_check=time.monotonic()
                    if time.monotonic()-self.last_sample>5:
                        snapshot=[]
                        for key in self.assigned:
                            low,high=self.consumer.get_watermark_offsets(TopicPartition(*key),cached=True)
                            progress=self.ledger.db.execute("SELECT committed_next_offset,safe_next_offset FROM partition_progress WHERE topic=? AND partition_id=?",key).fetchone()
                            committed=progress[0] if progress else None
                            snapshot.append(dict(topic=key[0],partition=key[1],log_start=low,log_end=high,committed_next_offset=committed,lag=None if committed is None or high<0 else high-committed))
                        self.offset_snapshot=snapshot
                        self.last_sample=time.monotonic()
                    if self.pending and (len(self.pending)>=self.settings.batch_size or
                        sum(len(c.row["value"])+len(c.row["key"]) for c in self.pending)>=self.settings.max_buffered_bytes or
                        (time.monotonic()-self.last_flush)*1000>=self.settings.flush_ms):
                        self.flush()
                    if self.state=="DEGRADED":
                        self.state="PROCESSING"
                        self.failure_reason=None
                        self.consumer.resume([TopicPartition(*p) for p in self.assigned])
                    message=self.consumer.poll(.2)
                    if message is None: continue
                    self._handle_msg(message)
                    self.state="PROCESSING"
                except IntegrityFault:
                    raise
                except Exception as exc:
                    self.state="DEGRADED"
                    self.failure_reason=str(exc)
                    self.consumer.pause([TopicPartition(*p) for p in self.assigned])
                    # Poll even during persistence failure so callbacks and ownership
                    # changes are serviced. A queued record is retained, never dropped.
                    queued=self.consumer.poll(.5)
                    if queued is not None and not queued.error():
                        self._handle_msg(queued)
        except Exception as exc:
            self.state="FAULTED"
            self.failure_reason=str(exc)
            if isinstance(exc,IntegrityFault):
                try:
                    self.ledger.db.execute("INSERT INTO integrity_faults VALUES(?,?,?)",("partition-fault",str(exc),self.ledger.now()))
                except Exception:
                    log.exception("could not persist integrity fault")
            log.exception("V1 worker fault")
        finally:
            if self.state!="FAULTED":
                try:
                    self.flush()
                except Exception:
                    log.exception("shutdown retains unresolved deliveries without committing")
            self.assigned.clear()
            self.consumer.close()

    def health(self):
        try:
            self.repo.client.command("SELECT 1")
            self.ledger.db.execute("SELECT 1")
            dispositions=dict(self.ledger.db.execute("SELECT destination,count(*) FROM deliveries WHERE confirmed_hash IS NOT NULL GROUP BY destination").fetchall())
            dependencies=True
        except Exception:
            dependencies=False
            dispositions={}
        ready=bool(self.assigned) and dependencies and self.broker_ok and self.state in ("READY","PROCESSING")
        return dict(state=self.state,ready=ready,assigned=sorted(self.assigned),
                    raw_deliveries=dispositions.get("RAW",0),quarantine_deliveries=dispositions.get("QUARANTINE",0),
                    buffered_records=len(self.pending),topic_identity=self.identity,
                    last_successful_persistence_time=self.last_persistence,
                    last_successful_commit_time=self.last_commit,
                    failure_reason=self.failure_reason,partition_offsets=self.offset_snapshot,**self.counts)

