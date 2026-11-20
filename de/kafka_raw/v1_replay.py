"""Bounded isolated Kafka replay; never commit offsets or write live tables."""
import argparse
import time
import uuid
from pathlib import Path
import clickhouse_connect
from confluent_kafka import Consumer, TopicPartition, TopicCollection
from confluent_kafka.admin import AdminClient
from de.kafka_raw.v1_config import V1RawSettings
from de.kafka_raw.v1_models import V1Record
from de.kafka_raw.v1_validator import V1RawValidator
from de.kafka_raw.v1_ledger_store import V1LedgerStore, IntegrityFault
from de.kafka_raw.v1_clickhouse_repository import V1ClickHouseRepository

def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument("--topic",required=True)
    parser.add_argument("--partition",required=True,type=int)
    parser.add_argument("--start-offset",required=True,type=int)
    parser.add_argument("--end-offset",required=True,type=int)
    parser.add_argument("--replay-run-id",required=True)
    parser.add_argument("--bootstrap-servers",default="localhost:29092")
    args=parser.parse_args(argv)
    if not args.replay_run_id or any(x not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for x in args.replay_run_id):
        parser.error("unsafe replay ID")
    if args.start_offset<0 or args.end_offset<args.start_offset:
        parser.error("invalid half-open source range")
    config=V1RawSettings()
    consumer=Consumer({"bootstrap.servers":args.bootstrap_servers,
        "group.id":"de-kafka-raw-v1-replay-"+args.replay_run_id,
        "isolation.level":"read_committed",
        "enable.auto.commit":False,"enable.auto.offset.store":False,"auto.offset.reset":"error"})
    tp=TopicPartition(args.topic,args.partition,args.start_offset)
    low,high=consumer.get_watermark_offsets(tp,timeout=10)
    if args.start_offset<low or args.end_offset>high:
        raise IntegrityFault("range outside retained Kafka offsets")
    admin=AdminClient({"bootstrap.servers":args.bootstrap_servers})
    identity=str(admin.describe_topics(
        TopicCollection([args.topic]))[args.topic].result().topic_id)
    client=clickhouse_connect.get_client(host=config.clickhouse_host,port=config.clickhouse_port)
    repo=V1ClickHouseRepository(client,args.replay_run_id)
    ledger=V1LedgerStore(str(Path("de/artifacts/kafka_raw_v1/replay")/args.replay_run_id/"ledger.sqlite3"))
    consumer.assign([tp])
    deadline=time.monotonic()+60
    records=[]
    try:
        while time.monotonic()<deadline:
            msg=consumer.poll(.5)
            if msg is None:
                if consumer.position([tp])[0].offset>=args.end_offset: break
                continue
            if msg.error(): raise RuntimeError(str(msg.error()))
            if msg.offset()>=args.end_offset: break
            kind,stamp=msg.timestamp()
            record=V1Record(args.topic,args.partition,msg.offset(),msg.key(),msg.value(),
                            tuple(msg.headers() or ()),stamp if stamp>=0 else None,kind,identity)
            records.append(V1RawValidator().classify(record))
            if msg.offset()+1>=args.end_offset: break
        if consumer.position([tp])[0].offset<args.end_offset:
            raise RuntimeError("replay timed out before requested end boundary")
        if records:
            ledger.receive_batch(records,uuid.uuid4().hex)
            ledger.transition([c.row["raw_ingestion_id"] for c in records],"PERSISTENCE_ATTEMPTED")
            found=repo.reconcile(records)
            known={c.row["raw_ingestion_id"] for c in found}
            repo.insert([c for c in records if c.row["raw_ingestion_id"] not in known])
            confirmed=repo.reconcile(records)
            if len(confirmed)!=len(records): raise RuntimeError("replay unresolved")
            ledger.transition([c.row["raw_ingestion_id"] for c in confirmed],"PERSISTENCE_CONFIRMED")
        # Publish bounded replay coverage only after every delivered record has
        # been positively reconciled. This metadata is replay-only; live ledgers
        # and broker group offsets remain untouched.
        ledger.db.execute('CREATE TABLE IF NOT EXISTS replay_source_ranges(topic TEXT, partition_id INTEGER, topic_identity TEXT, start_offset INTEGER, end_offset INTEGER, PRIMARY KEY(topic,partition_id))')
        old=ledger.db.execute('SELECT * FROM replay_source_ranges WHERE topic=? AND partition_id=?',(args.topic,args.partition)).fetchone()
        if old and tuple(old)!=(args.topic,args.partition,identity,args.start_offset,args.end_offset):
            raise IntegrityFault('replay coverage cannot be replaced with another range')
        ledger.db.execute('INSERT OR IGNORE INTO replay_source_ranges VALUES (?,?,?,?,?)',
            (args.topic,args.partition,identity,args.start_offset,args.end_offset))
        ledger.db.execute('INSERT OR REPLACE INTO partition_progress VALUES (?,?,?,?,?)',
            (args.topic,args.partition,identity,None,args.end_offset))
        # A completed replay is a quiescent read-only source. Checkpoint WAL
        # and leave rollback-journal mode so readers on a :ro directory do not
        # need to create SHM files. Live Raw stays in WAL mode.
        ledger.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        ledger.db.execute('PRAGMA journal_mode=DELETE')
        print({"replay_run_id":args.replay_run_id,"deliveries":len(records),"live_commits":0})
    finally:
        consumer.close()
        client.close()
        ledger.close()

if __name__=="__main__":
    main()

