"""Batch persistence and full-row physical reconciliation."""
import json
from collections import defaultdict
from de.kafka_raw.v1_models import encoded
from de.kafka_raw.v1_ledger_store import IntegrityFault

class V1ClickHouseRepository:
    def __init__(self, client, replay_id=None):
        self.client=client
        self.replay_id=replay_id

    def table(self, destination):
        return "smart_traffic.v1_"+destination.lower()+"_events"+("_replay" if self.replay_id else "")

    def verify_tables(self):
        from de.kafka_raw.v1_models import V1Record
        from de.kafka_raw.v1_validator import V1RawValidator
        sample=self.materialize(V1RawValidator().classify(V1Record("schema-check",0,0,None,None)))
        required=set(sample)-{"replay_run_id"}
        for suffix in ("raw_events","quarantine_events","raw_events_replay","quarantine_events_replay"):
            result=self.client.query("DESCRIBE TABLE smart_traffic.v1_"+suffix)
            types={row[0]:row[1] for row in result.result_rows}
            expected=required|({"replay_run_id"} if suffix.endswith("_replay") else set())
            if not expected.issubset(types):
                return False
            if any(types.get(name)!="FixedString(64)" for name in
                   ("raw_ingestion_id","physical_record_hash","payload_bytes_hash")):
                return False
        return True

    def materialize(self, classified):
        r=classified.row
        row=dict(topic=r["topic"],partition=r["partition"],offset=r["offset"],
            raw_ingestion_id=r["raw_ingestion_id"],topic_identity=r["topic_identity"],
            physical_record_hash=r["physical_record_hash"],destination=classified.destination,
            payload_bytes_hash=r["payload_bytes_hash"],value_is_null=int(r["value_is_null"]),
            key_is_null=int(r["key_is_null"]),value_bytes_base64=r["value"],
            key_bytes_base64=r["key"],headers_json=json.dumps(r["headers"],separators=(",",":")),
            broker_timestamp_ms=r["timestamp_ms"],broker_timestamp_type=r["timestamp_type"],
            consumed_at=r["consumed_at"],event_id=r["event_id"],payload_hash=r["payload_hash"],
            simulation_run_id=r["simulation_run_id"],intersection_id=r["intersection_id"],
            immutable_envelope_hash=r["immutable_envelope_hash"],envelope_json=r["envelope_json"],
            payload_text=r["payload_text"],validator_version=r["validator_version"],
            migration_version=r["migration_version"],failure_stage=classified.failure_stage or "",
            error_code=classified.error_code or "",error_detail=classified.error_detail or "")
        if self.replay_id: row["replay_run_id"]=self.replay_id
        return row

    def insert(self, records):
        for dest in ("RAW","QUARANTINE"):
            rows=[self.materialize(c) for c in records if c.destination==dest]
            if rows:
                columns=list(rows[0])
                self.client.insert(self.table(dest),[[r[c] for c in columns] for r in rows],
                                   column_names=columns,settings={"async_insert":0})

    def evidence(self, ids):
        found=defaultdict(list)
        if not ids: return found
        # ClickHouse HTTP form parameters have bounded field sizes. Large
        # startup/read ranges must use bounded batches, not one huge IN array.
        if len(ids)>500:
            for start in range(0,len(ids),500):
                for rid,rows in self.evidence(ids[start:start+500]).items():
                    found[rid].extend(rows)
            return found
        for dest in ("RAW","QUARANTINE"):
            params={"ids":list(ids)}
            clause="raw_ingestion_id IN {ids:Array(String)}"
            if self.replay_id:
                params["replay"]=self.replay_id
                clause+=" AND replay_run_id={replay:String}"
            q=self.client.query("SELECT * FROM "+self.table(dest)+" WHERE "+clause,parameters=params)
            for values in q.result_rows:
                row=dict(zip(q.column_names,values))
                for field in ("raw_ingestion_id","physical_record_hash","payload_bytes_hash"):
                    if isinstance(row[field], bytes):
                        row[field]=row[field].decode("ascii")
                found[row["raw_ingestion_id"]].append(row)
        return found

    def reconcile(self, records):
        found=self.evidence([c.row["raw_ingestion_id"] for c in records])
        confirmed=[]
        for c in records:
            rows=found[c.row["raw_ingestion_id"]]
            if not rows: continue
            expected=self.materialize(c)
            for row in rows:
                for key in expected:
                    if key=="consumed_at": continue
                    if row[key]!=expected[key]:
                        raise IntegrityFault("ClickHouse physical mismatch: "+key)
            confirmed.append(c)
        return confirmed

    def logical_read(self, ledger, topic, partition, start_offset, end_offset=None):
        args=[topic,partition,start_offset]
        sql="SELECT * FROM deliveries WHERE topic=? AND partition_id=? AND offset_value>=?"
        if end_offset is not None:
            sql+=" AND offset_value<?"
            args.append(end_offset)
        deliveries=ledger.db.execute(sql+" ORDER BY offset_value",args).fetchall()
        evidence=self.evidence([r["raw_ingestion_id"] for r in deliveries])
        result=[]
        for d in deliveries:
            rows=evidence[d["raw_ingestion_id"]]
            confirmed=d["state"] in ("PERSISTENCE_CONFIRMED","COMMIT_ELIGIBLE","COMMITTED")
            status="UNRESOLVED"
            if rows and confirmed:
                pairs={(r["physical_record_hash"],r["destination"]) for r in rows}
                status=("CONFIRMED_"+d["destination"] if pairs=={(d["physical_record_hash"],d["destination"])}
                        else "CONFLICT")
                if status.startswith("CONFIRMED"):
                    # A stored hash is an assertion, not proof that every physical
                    # retry row has identical immutable content.
                    immutable={k for k in rows[0] if k not in ("consumed_at", "failed_at")}
                    if any(any(row.get(k) != rows[0].get(k) for k in immutable) for row in rows[1:]):
                        status="CONFLICT"
                if status.startswith("CONFIRMED") and "row_json" in d.keys():
                    source=json.loads(d["row_json"])
                    expected={"topic":source["topic"],"partition":source["partition"],
                        "offset":source["offset"],"raw_ingestion_id":source["raw_ingestion_id"],
                        "topic_identity":source["topic_identity"],
                        "physical_record_hash":source["physical_record_hash"],
                        "payload_bytes_hash":source["payload_bytes_hash"],
                        "value_is_null":int(source["value_is_null"]),
                        "key_is_null":int(source["key_is_null"]),
                        "value_bytes_base64":source["value"],
                        "key_bytes_base64":source["key"],
                        "headers_json":json.dumps(source["headers"],separators=(",",":")),
                        "broker_timestamp_ms":source["timestamp_ms"],
                        "broker_timestamp_type":source["timestamp_type"]}
                    if any(any(row.get(k)!=v for k,v in expected.items()) for row in rows):
                        status="CONFLICT"
            progress=ledger.db.execute("SELECT safe_next_offset FROM partition_progress WHERE topic=? AND partition_id=?",(topic,partition)).fetchone()
            result.append(dict(identity=(topic,partition,d["offset_value"]),status=status,
                safe_next_offset=progress[0] if progress else None,
                physical_rows=len(rows),record=rows[0] if status.startswith("CONFIRMED") else None))
        return result

