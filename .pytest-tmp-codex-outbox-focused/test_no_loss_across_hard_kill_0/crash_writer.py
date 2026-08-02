
import json, os, sys
sys.path.insert(0, sys.argv[2])
sys.path.insert(0, sys.argv[3])
from integration.kafka.outbox_store import KafkaOutboxStore, OutboxRow

store = KafkaOutboxStore(sys.argv[1])
rows = [
    OutboxRow(
        event_id=f"{c:04d}{i:060d}",
        simulation_run_id="run-crash",
        cycle_sequence=c,
        entity_sequence=i,
        event_key="run-crash:A",
        topic="traffic.entity-events.v2",
        payload_json=json.dumps({"eventId": f"{c:04d}{i:060d}"}),
        payload_hash="b" * 64,
    )
    for c in range(3)
    for i in range(8)
]
for c in range(3):
    store.append_cycle(rows[c * 8:(c + 1) * 8])
store.mark_queued_batch([r.event_id for r in rows[:4]])
# Hard kill: no close(), no checkpoint, no interpreter cleanup.
os._exit(9)
