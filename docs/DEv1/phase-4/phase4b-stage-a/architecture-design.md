# Kiến trúc Gold Business Stage A

## Baseline đã xác minh

Plan V2 là commit e57116e5bdda9fc6c39fdf84ae83c78dcd780cbc. Source dưới đây là hành vi hiện tại. Các lane, schema Business và giao thức trong phần sau là thiết kế chờ duyệt.

- Silver sở hữu public feed. [Phase 4A Pre contract](../pre/contract-design.md) và de/silver/incremental_reader.py:_build_feed_page quy định bound hữu hạn theo trang, contiguous verified prefix, independent invalidations và conflict dispositions. Không có snapshot xuyên các request.
- de/gold_runtime/main.py dùng GoldCoreProcessor. _bootstrap xác minh binding/cursors trước work. run_cycle gọi discovery → invalidation → change → Core work, dừng khi mất lease hoặc vượt disk guard. _changes chỉ ACK khi cursor invalidation đạt fence của receipt. CoreWorkLedger.acknowledge/_apply cập nhật receipt, suppression/membership và cursor durable.
- CoreInputStream đọc indexed members theo event ID, mỗi member qua receipt public đúng sequence, recheck generation, bundle/transition identity và invalidation high water, kiểm tra lần cuối khi exhaustion. _work hiện bỏ bundle, finish CAS chỉ ghi proof. Core scheduler claim RUNNING/PENDING. core_window key không chứa scenario. Core completeness UNKNOWN và finality DISABLED.
- de/gold_runtime/repositories.py là đường persistence legacy, gồm lookup FINAL. Active Core không publish marts. Không kế thừa FINAL hoặc latest-row làm authority.

## Flow đề xuất và điểm tách lane

```
Silver verified pages → Core recovery / invalidation / contiguous ACK
                                      ↓
                      Core work + verified member stream
                              ↓ bounded slot projection
                    [SQLite: finish CAS + obligation + projection]
                              ↓                 ↖ startup recovery
                      Business compute lane (CPU)
                              ↓
                    [SQLite: durable computed result]
                              ↓
                      Business publication lane ↔ ClickHouse exact rows
                              ↓ verified receipt set
                    [SQLite: current pointer + epoch CAS]
                              ↓
                   Gold reader pin → CH exact IDs → revalidate → response
Replay: frozen public source → isolated Core and Business stores/namespace
        → separate Core / compute / publication completion predicates
```

Giữ một verified traversal trên normal path. Core thu projection nhỏ cần cho slot ledger khi đọc member. Chỉ sau verified exhaustion mới seal projection cùng proof. Business compute đọc projection, không tự xác minh lại theo semantic khác. So với tính toàn bộ metrics inline, tách compute giúp giới hạn CPU sort/rollup và retry trên đường Core, đổi lại thêm bytes/WAL. Staging từng member sẽ kéo dài số giao dịch và tăng orphan state. Reread theo KPI hoặc read private tables bị loại. Requalification public chỉ dành cho designated historical rebuild, recovery trước atomic commit hoặc mất/corrupt projection theo quy trình có owner approval. Xem [state and migration](state-schema.md).

## R01/R09: scheduler, quyền sở hữu và admission

Một coordinator giữ Core lease và là SQLite writer duy nhất trong mỗi namespace. Compute worker nhận immutable projection và token, trả result về coordinator qua mailbox bounded. Publication I/O có client riêng, deadline/cancellation và batch caps; worker không sở hữu pointer hoặc SQLite writer. Core loop không đợi worker/ClickHouse. Baseline đề xuất một job compute và một job publication đang chạy mỗi live namespace, cần xác nhận bằng benchmark trước vận hành. Tách execution lane chưa đủ cô lập CPU, I/O, ClickHouse server và WAL; đo cả realtime service co-tenancy.

1. Startup chạy schema check, kiểm tra lease và bootstrap Core, sau đó reconcile stale Business claims và result hashes theo bounded cursor. Change ACK vẫn phải chờ invalidation qua receipt fence. Business retry không được giữ lock hoặc vượt thứ tự Core.
2. Mỗi Core tick giữ các quantum discovery/invalidation/change/work hiện có. Sau Core tick coordinator dùng lượng thời gian và số transaction được cấp để dispatch/nhận Business work. Core member verification tiếp tục theo quantum hiện có. Projection size được reserve trước claim hoặc spool theo một giới hạn bytes in-memory, validate cả khi actual cardinality vượt estimate.
3. Trong mỗi lane chọn việc đến hạn theo (ready_at, created_at, stable ID), gồm recoverable live và fresh live. Dùng credit luân phiên giữa retry đã đến hạn và fresh work để một poison item không chặn queue. Hàng đợi SQLite indexed, mailbox và in-flight quota tách. Timeout, exponential backoff có jitter, attempt/deadline budget theo lỗi. Exhausted/permanent lỗi chuyển BLOCKED giữ evidence, chỉ manual rearm có lý do. Generation mismatch supersede thay vì retry vô hạn.
4. Replay/backfill dùng namespace riêng và ngân sách CPU/I/O/CH riêng, chỉ chạy phần dư sau live reserve. Nếu trong envelope có replay credit dương, cấp quantum theo lượt cho eligible replay; khi live pressure vượt guard, pause replay và công bố reason/age. Không bảo đảm replay progress trong lúc live đã hết headroom. Replay không sở hữu live cursor hoặc pointer.
5. Admission tính queue count, projection/result bytes, DB+WAL, free-disk reserve, oldest age và retry due. Reserve cả projection và worst-case computed payload cùng thời điểm cho work mới, giữ riêng WAL and Core reserve. Một admitted compute được dùng reservation của nó ngay cả khi admission đã đóng, tránh deadlock drain. Khi high watermark: ngừng historical admission trước, ưu tiên drain live publish rồi compute; giữ Core receipt/invalidation progress và Core work admissible trong reserve. Khi Business cap chạm, defer Core finish của work chưa có slot durable; giữ OPEN/PENDING/RUNNING theo ledger semantics, không tạo COMPLETE giả. Nếu actual projection vượt hard bound, dừng work ở trạng thái có thể phục hồi và báo fault; không truncate input.
6. Core ACK còn tăng Core state dù Business bị pause. Trước khi đụng protected Core/WAL reserve, drain hoặc stop intake/work có kiểm soát ở durable cursor, báo degraded/fault. Resume khi dependency khỏe, outstanding age/bytes giảm qua low watermark theo window quan sát và có đủ reserve. Không skip prefix, drop pending work, hoặc giữ intake vô hạn khi disk cạn.

Hard constraints là single writer, bounded allocations, admission trước durability, no loss, fence checks và no false COMPLETE. Các con số budgets, watermarks, quanta, retry limits và minimum Core/realtime service chưa duyệt. Production topology/cadence và concurrent workload là input của benchmark, không lấy threshold từ diagnostic fixture. [Performance và overload](performance-design.md).
