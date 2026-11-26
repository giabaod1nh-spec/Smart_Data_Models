# Business publication, revision and recovery design

## Separate state dimensions

Do not overload Core `COMPLETE` as business `PUBLISHED`.

1. **Source completeness:** expected observations and per-metric coverage are evaluated against approved schedule/population.
2. **Core work:** work generation and verified source proof complete for the recorded input fence.
3. **Business result:** deterministic metric calculations and cross-grain rollups complete for pinned metric versions.
4. **Business revision:** monotonic per logical result key, tied to Core generation, topology, definition set and proof identity.
5. **Publication:** staged rows verified; one durable manifest/fence marks an entire compatible result family eligible.
6. **Serving eligibility:** query selects current published manifest only if generation/source fence is not suppressed by a known invalidation.

Candidate lifecycle: `NOT_ELIGIBLE → COMPUTING → STAGED → VERIFIED → PUBLISHED`; failure → `FAILED` with no visibility; source invalidation/generation change → `SUPPRESSED/REPROCESSING`; corrected output creates a new revision. Partial metric availability can be published only if approved metric-specific quality policy allows it, with explicit per-metric states and no blending of old/new rows.

## Atomic visibility across SQLite and ClickHouse

There is no distributed transaction. Proposed protocol:

1. Under local SQLite transaction and current owner/generation fence, create deterministic result identity and `STAGING` manifest with expected output grains/count, proof identity and definition versions.
2. Compute output bytes/rows deterministically from one verified input proof. Record digest and stage immutable rows in ClickHouse with deterministic insert token/row identity where supported. Retry ambiguous inserts idempotently; never assume a timeout means failure.
3. Read back by immutable output identity and verify row count, keys, values/digest and proof lineage independently of the aggregation code.
4. In a SQLite transaction, recheck generation, suppression, invalidation cursor/fence and owner lease; mark manifest `PUBLISHED` and advance current-effective pointer only if every expected grain verifies.
5. Serving resolves exactly one manifest/revision and filters all products by it. It never independently takes latest revision from each table.

Crash after ClickHouse insert but before manifest publish leaves invisible staged rows. Recovery re-verifies and publishes if fences still hold, otherwise marks them orphaned/suppressed for retention cleanup. Crash after SQLite publish returns the same effective manifest. If source invalidation arrives later, suppression is durable locally before new work; serving must consult the effective manifest/fence so invalidated revision is not selected after Gold observes the invalidation. The design cannot promise instantaneous source-to-serving suppression before Silver invalidation is observed.

## Failure matrix

| Failure point | Durable state | May serve? | Recovery and expected result |
|---|---|---|---|
| Before stage manifest | No business obligation | Existing prior published revision only if its fence remains valid | Retry current generation from verified source |
| After `STAGING`, before CH write | Manifest only | No new result | Resume same deterministic ID or fail it; no duplicate revision |
| CH insert rejected | Manifest + partial/none rows | No staged result | Retry bounded insert; verify all expected rows |
| CH accepted, client timed out | Possibly all rows; outcome ambiguous | No new result | Query deterministic identity, compare expected digest, then continue or repair |
| Some grains inserted | Partial rows + `STAGING` | No new result family | Insert missing rows, verify whole family; never expose individual grain early |
| Read-back/validation mismatch | Staged rows + failed validation | No new result | Quarantine result identity, preserve evidence, recompute only after cause resolved |
| Crash before publication transaction | Verified rows, unpublished manifest | No new result | Recheck generation/fence then publish or suppress |
| Crash after publication transaction | Published manifest and pointer | Yes, all products under same manifest | Idempotent restart returns same revision |
| Invalidation observed before publication | Suppression/fence durable | No invalidated result | Discard/stage retained data as orphan; recompute new generation |
| Invalidation arrives after publication | Existing published manifest then new suppression | Once suppression is durable, prior result unavailable/current status reprocessing | Create corrected revision; never blend products |
| Replay run | Isolated namespace/database/lock | No live result | Replay produces deterministic isolated manifest; no live writes |

## Required invariants and oracle

All direction/intersection/network rows served together must carry one publication manifest, run generation, topology, definition set and business revision. A missing metric cannot borrow its value from an older revision. Use idempotent result IDs; conflicting output for same ID is an integrity fault. Replay state is isolated and V2 is untouched. An independent source-derived oracle checks expected slot set, values, row identities, digests and effective selection. Crash tests inspect both SQLite manifests and ClickHouse physical rows plus serving results.

## Protocol bổ sung (RV-07/09/10)

SQLite do Gold coordinator sở hữu. Obligation key gồm namespace, Core work ID, generation, input hash, source binding, definition set, source semantics version và population/schedule digest. Unique key chặn cấp revision lặp. Immutable result ID hash canonical obligation key + assigned revision. Expected row set là exact grain/entity keys cộng content hashes và row count cho mỗi grain. Durable transaction finish + obligation là một đơn vị: crash trước commit để RUNNING và replay work, sau commit obligation luôn tồn tại dù worker chưa gửi CH. Quét đối chiếu bounded theo keyset phát hiện proof cũ hoặc lỗi invariant.

Writer đọc exact result ID trước retry insert mơ hồ. Read-back đối chiếu canonical payload từng key với manifest, chỉ retry bổ sung key thiếu dưới writer lease. Bản sao byte-identical được gộp theo key khi đọc và ghi cảnh báo, kể cả sau thời hạn insert-token. Hai payload khác nhau cùng key là integrity fault, cách ly cả revision. Đối soát tất cả grain trước transaction đổi pointer. Không lấy MAX(revision), insertion time hoặc merge làm authority. Cleanup staged/orphan chỉ khi fence xác định không còn quyền publish và retention/read pin đã hết. Sau invalidation/late event, transaction Gold bump fence và suppress pointer liên quan, outstanding stages mất quyền publish. Nguồn thay đổi trước khi Gold nhận invalidate có khoảng eventual consistency.

Serving qua module Gold reader sở hữu truy cập SQLite read-only với transaction ngắn, CH read-only. Stage B gọi giao diện Gold logic này qua adapter/transport cần owner duyệt. CH-only views không kiểm tra được pointer. CH mirror của authority có chi phí đồng bộ và stale-read, chỉ xem xét nếu đo được cần thiết. Ưu tiên reader module với một adapter được cấp quyền, chưa yêu cầu service mới hay Spring truy cập SQLite trực tiếp.

1. Trong cùng SQLite snapshot, resolve scope/range, exact manifest IDs kể cả gaps và pin fence token gồm publication epoch theo run/duration và timeline/definitions digest.
2. Kết thúc transaction rồi query CH bằng exact result IDs. Validate keys, digests, grain counts và duplicate/conflict rules, chỉ trả projections sau verify.
3. Mở snapshot SQLite mới, revalidate cùng fence/pointer set trước khi trả response. Mốc đọc là lần kiểm tra cuối thành công trong local authority, không thu hồi được response đã phát. Không claim distributed snapshot.
4. Fence đổi trong lúc đọc → retry trong retry/deadline budget hoặc REPROCESSING. Authority outage → UNAVAILABLE, không fallback CH/cache. Revision 1 suppressed + revision 2 staged → không có effective result. Multi-window pin cả set trong một snapshot; từng window có revision riêng. KPI/rank/cell chia sẻ selection token. Cursor theo range, sort key và fence. Nếu revision đổi giữa pages thì trả STALE_CURSOR và bắt đầu lại. Cache theo exact IDs + fence, revalidate authority mỗi hit. Coarse epoch có thể retry vì đổi ngoài range, đo trước khi chuyển sang fine-grained validation.

| Barrier bổ sung | Trạng thái và recovery | Acceptance |
|---|---|---|
| Trước/sau Core finish CAS | Cả proof và obligation commit cùng nhau hoặc không có; restart claim work hoặc drain durable obligation | SA-09 |
| Fence đổi khi staging (late/invalidate) | Suppress; discard stage khỏi effective set; worker generation mới tính lại | AC-16/18 |
| CH duplicate/conflicting rows | Identical dedup, divergent digest fail closed; rebuild có quản trị | SA-17 |
| Invalidation/range revision race trong khi serving | Retry hoặc REPROCESSING/STALE_CURSOR; không trộn manifest set | SA-13/14 |
| Authority mất kết nối | UNAVAILABLE kể cả cache còn warm; retry bounded | SA-18 |

## Historical proofs và rebuild

Scan Core proof index theo current generation, run/range và durable keyset cursor riêng. Barriers ghi cursor và obligation idempotent cùng transaction. Historical proof chỉ là discovery input. Backfill cần source/binding/version, lịch và retention tương thích. Đọc lại indexed members qua public Silver, reverify proof/generation/hash/fence. Proof cũ lệch current source thì dùng current generation đủ bằng chứng, hoặc báo unsupported/retention gap, giữ evidence. Quota theo windows, bytes, wall budget, RSS và priority live. Replay dùng namespace riêng. Late data tạo generation mới. Invalidation suppress current. Definition upgrade cùng proof nhưng definition set mới. Source semantics hoặc coverage policy đổi thì key compatibility mới và explicit rebuild/selection approval. Không diễn giải lại rows cũ.
