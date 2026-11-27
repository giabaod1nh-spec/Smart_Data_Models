# Publication, recovery và replay

## R06/R07: publish durable result qua một authority

[State machine và identity](state-schema.md) quy định atomic obligation, COMPUTED và immutable result/revision. ClickHouse hiện chưa có active V1 writer. Marts ứng viên dùng append-only rows với (result ID, grain, row key, digest). Gold coordinator giữ SQLite publication authority duy nhất, writer I/O dùng short-lived claim token và deadline, không ghi pointer. SQLite và ClickHouse không dùng chung transaction.

1. Coordinator claim PENDING theo live budget, recheck generation/token, validate payload/schema/digest, chuyển WRITING. Worker dùng expected row manifest và batch count/byte caps. Negative generation check trước dispatch giảm stale writes, còn CAS cuối mới quyết định visibility.
2. Đọc exact keys trước insert/retry. Nếu ambiguous timeout, reconcile đủ các grain: missing keys được append, duplicate identical gộp theo key, divergent/unexpected key cách ly result. Không dựa vào insert token sau TTL, FINAL hay eventual merge. Verify cả keys/count/digest bằng exact read-back với read consistency của deployment đã duyệt. Nếu discrepancy hoặc replica chưa thấy kết quả, không publish. Publisher không tái compute.
3. Coordinator ghi VERIFIED với verified row digest chỉ khi claim/lease, scope, generation, proof và suppression fence match. Trong transaction publish, recheck VERIFIED và current generation/policy, set pointer, bump epoch và PUBLISHED atomic. Invalidation ACK hoặc late change đã xác nhận phải bump relevant epoch và clear prior pointer trong cùng Core-local transaction. Late writer có thể để lại immutable orphan nhưng không thể publish revision cũ.
4. Phục hồi WRITING qua pending reconciliation. Crash sau CH write trước SQLite commit để staged rows invisible. VERIFY/PUBLISH retry dùng cùng result và manifest. Storage cap/CH failure báo degraded và giữ payload cho tới terminal approved. Cleanup của orphan dùng bounded sweep, chờ expired claims, in-flight CH deadlines/grace, no current pointer, no read pin và retention gate. Late inserts sau cleanup phải vẫn invisible và nhận diện lại được qua result identity/tombstone.
5. Gold reader pin exact manifest set, expected timeline và scope epochs trong một SQLite snapshot, release lock, query CH bằng exact IDs. Verify payload/projection/digests và grain set, rồi revalidate scope epochs/selectors trong snapshot mới trước return. Điểm đọc là lần validate authority cuối. Range/experiment recheck chung cả set. Nếu fence đổi, bounded retry hoặc REPROCESSING/STALE_CURSOR. Authority unavailable trả UNAVAILABLE cả cache warm. Revision cũ suppressed + revision mới staged cho no effective result. Không claim instant propagation trước Gold invalidation ACK. [Serving contract](serving-contract.md).

| Failure | Persistent outcome và recovery | FA |
|---|---|---|
| CH down hoặc deadline | WRITING → PENDING, COMPUTED preserved; per-dependency backoff | 15 |
| Timeout nhận insert hoặc partial grain | Exact reconcile, append missing, verify family trước pointer | 16 |
| Duplicate identical/divergent key | Gộp identical, divergent BLOCKED, no pointer | 17 |
| Crash sau CH insert | Staged hidden, same revision on restart | 18 |
| Invalidation/late generation trong compute/write | Token rejected, old current cleared atomic, orphan hidden | 19 |
| Reader race, authority lost | Revalidate or fail closed; range cannot mix selectors | 20 |

## Historical proofs, version rebuild và replay (R08)

Current CoreReplayManifest gắn source binding, fully applied frozen (Hc,Hi,Sc,Si), run generation/topology/manifest, scope và versions. replay.py:check reject thay đổi boundary. GoldCoreProcessor.run_cycle đánh dấu Core COMPLETE khi đạt frozen cursor, work lane đã yên và không có fault. Terminal Core shortcut hiện chấm dứt Core loop. Thiết kế Business runner có phase/tick độc lập sau Core terminal. Không nới Core replay rules hay ép live producer dừng. Nếu source live advances làm frozen authority khác, giữ Core behavior fail/abort. Muốn historical replay song song ổn định cần separately approved immutable captured source đọc qua public contract tương thích. Scheduler cap và pause replay khi live pressure.

Replay ID Business = hash(Core replay ID, frozen schedule/lifecycle, metric/coverage/source/projection versions, explicit eligibility scope). Persist bounded keyset expected work shards từ approved schedule và frozen Core window/generation index. Seal set digest sau Core COMPLETE và xác nhận cả missing/terminal statuses theo timeline. Required full windows cần effective proof và obligation; missing/unsupported/conflict phải có disposition và làm scope incomplete nếu required. Terminal partial/excluded chỉ có thể loại theo rule đã được frozen trước. Không chọn scope chỉ từ results. Replay DB, leases, publication prefix và CH namespace tách live. Scans, retries và cleanup scoped tới replay ID.

- Core completion cần Core predicate hiện hành và persisted COMPLETE.
- Compute completion chỉ khi mỗi required current-generation key có valid durable COMPUTED payload. Pending, retries, unresolved conflicts hoặc BLOCKED không count.
- Publication completion chỉ khi mỗi required key có verified, effective PUBLISHED pointer trong replay namespace. Nếu job mode compute-only được duyệt thì publication = NOT_REQUESTED, overall status ghi COMPUTE_ONLY_COMPLETE.
- Overall full replay COMPLETE cần đủ ba predicates, same expected set digest và last local fence CAS. SUPERSEDED lineage giữ audit và required key vẫn phải có current replacement trong đúng frozen set. Nếu correction thuộc boundary mới, abort replay cũ và lập job ID mới, không thêm đến vô hạn.
- Restart đọc sealed set/checkpoints, revoke stale claims, dùng durable projections/results và resume stage. Correction/stale authority reject commit. Sau activation, preexisting Core proofs scan theo bounded keyset riêng, verify public requalification, compare input_hash/current generation và record obligation idempotent. Retention gap và unsupported policy báo BLOCKED/UNSUPPORTED. Version rebuild có identity mới, giữ old evidence, không reset Core or overwrite old interpretations.
