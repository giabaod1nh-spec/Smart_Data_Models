# Phase 3 recovery evidence

Focused tests cover duplicate reuse, conflicting event rollback, archive tampering, active-run generation fencing, failed structural-cycle recording, broker failure, and worker restart from an orphaned queued row. A fake Producer test verifies that the worker sends exactly the stored key and bytes and records broker ACKs. Another test simulates a crash after a broker ACK but before the SQLite ACK: recovery resends the identical stored bytes and reaches ACKED state.

The local broker integration used a short headless SUMO run with five intersections and a second run with `--nodes A,B`. `Visualize/tools/verify_v1_kafka.py` consumed the V1 topic with a temporary verifier group and compared Kafka keys and values with the Outbox. All 17 observed messages matched and were ACKED.

The tests simulate the ACK/crash gap in process but do not kill an actual OS process or fill a disk. Those paths retain the existing worker's at-least-once retry behavior and are documented in `outbox-semantics.md`.
