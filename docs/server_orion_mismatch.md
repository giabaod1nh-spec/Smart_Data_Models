# Server ↔ Orion: Lỗi và cách xử lý

Tài liệu ghi lại **vì sao** Spring Server (`:8080`) và dữ liệu Orion (`:1026`) / luồng Visualize **không khớp**  và **đã xử lý ngắn gọn thế nào**.

**Phạm vi:** chỉ module `server/`. Không sửa `Visualize/`, `contracts/`, Orion Publisher.

---

## Kiến trúc thực tế (Producer vs Consumer)

```text
Visualize (SUMO + TraCI)
  ├── publish NGSI-LD ──► Orion :1026
  └── Control API ──────► :9090  (/scenario, /phase, …)

Spring Server :8080  (trước khi sửa)
  └── chỉ GET entity từ Orion ──► map một phần sang DTO
```

Visualize là **producer** (ghi Orion + điều khiển sim). Server là **consumer + BFF**. Lệch xảy ra khi consumer không bám contract/pipeline mà producer đã publish.

---

## Triệu chứng hay gặp khi test (Postman / frontend)

| Triệu chứng | Người dùng thường hiểu nhầm |
|-------------|----------------------------|
| Gọi `:8080` không có `/scenario` | Tưởng server “thiếu API sim” |
| Orion / raw NGSI có `scenarioId`, REST `:8080` không có | Tưởng Orion sai hoặc server đọc sai broker |
| Đổi scenario trên `:9090` nhưng UI qua `:8080` không điều khiển được | Hai base URL, không một cổng |
| Entity Orion đủ field Contract v1, JSON API server thiếu field | “Server và Orion không khớp contract” |

---

## Nguyên nhân gốc (trước sửa )

### 1. Mapper / DTO chưa cover RT-DE Contract v1

Orion (do Visualize publish) đã có các field chung và theo entity, ví dụ:

- Chung: `simulationTime`, `simulationRunId`, `scenarioId`
- Intersection: `currentPhase`, `derivedTrafficState`, `hasSpillback`, …
- TrafficLight / VehicleSensor / Camera: field bổ sung theo `contracts/entity/`

Server ban đầu chỉ map **subset** field mô hình cũ (tên, location, ref\*, vài counter). **Không deserialize** các property Contract v1 → REST JSON **thiếu field** so với golden trong `contracts/entity/payloads/` dù Orion đúng.

**Đây là lệch chính “server ≠ Orion” trên mặt dữ liệu đọc.**

### 2. Không có proxy Control API trên `:8080`

Scenario **không nằm trên Orion**. Endpoint thật:

- `GET/POST /scenario` trên Visualize **`:9090`**

Server Phase 0 **không** expose `/api/control/scenario` → gọi Postman vào `:8080` **không thể** đổi scenario; phải biết thêm port `9090`. Đó là lệch **API surface**, không phải lỗi Orion.

### 3. Hai vai trò API chưa gom về một BFF

| Nhu cầu | Trước plan | Sau Phase 1 |
|---------|------------|-------------|
| Đọc entity | `:8080` → Orion | `:8080` → Orion (đủ field) |
| Điều khiển scenario | `:9090` trực tiếp | `:8080/api/control/**` → `:9090` |
| Health tổng | Rải rác | `:8080/api/system/health` |

Frontend/Postman chưa có **một base URL** cho Realtime quan sát + điều khiển.

### 4. Cấu hình Orion chưa tách health / profile

Plan yêu cầu:

- `orion.api-base-url` (NGSI-LD API)
- `orion.health-url` (riêng, ví dụ `/version`)
- Profile `local` / `docker` qua `SPRING_PROFILES_ACTIVE`, không hard-code trong repo

Trước đó dễ cấu hình lệch host (localhost vs Docker service name) hoặc check health không nhất quán — **không** làm sai entity trên Orion nhưng gây “server DOWN / Orion unreachable” khi deploy khác môi trường dev.

### 5. Không có aggregate + consistency

Orion chỉ lưu **state hiện tại** từng entity; publish lần lượt có thể lệch `simulationTime` ngắn hạn giữa Intersection và sensor con.

Server trước plan **không** có `GET /api/realtime/intersections/{id}` với:

- `cameras[]` (theo `refCameras`)
- `metadata.consistent` + `consistencyIssues`
- so sánh `simulationTime` với tolerance

→ Khó verify “scenario đã **applied**” sau `POST` (`queued: true`) chỉ bằng một intersection GET đơn lẻ.

### 6. Semantics lệnh bất đồng bộ chưa được document rõ

Control API trả `{"queued": true}` — lệnh vào CommandQueue, SUMO apply sau.

Nếu so sánh ngay `POST /scenario` với `GET /api/intersections/A` mà chưa đợi publish → thấy `scenarioId` cũ → **tưởng** server và Orion lệch, trong khi là **độ trễ apply + publish**.

---

## Cách đã xử lý (tóm tắt)

| Vấn đề | Xử lý |
|--------|--------|
| DTO thiếu Contract v1 | Bổ sung field vào 4 `*Response` + `NgsiEntityMapper`; test matrix với golden copy từ `contracts/entity/payloads/` |
| Không gọi scenario qua `:8080` | `ControlProxyController` + allowlist 16 route → `ControlApiClient` forward tới `:9090` |
| Hai cổng API | Một BFF `:8080`: entity read + `/api/control/**` + health |
| Cấu hình | `application-local.properties` / `application-docker.properties`; `orion.api-base-url` + `orion.health-url` |
| Verify applied | `GET /api/realtime/intersections/{id}` + tolerance + retry 1 lần + `consistencyIssues` |
| `queued` vs applied | Ghi trong `server/README.md` và `docs/implementation/server_postman_guide.md` |
| An toàn proxy | Allowlist path, lọc header, GET/POST/DELETE, giới hạn body JSON |
| Regression | WireMock (CI luôn chạy) + smoke `@Tag("live")` khi stack thật |

**Không đổi** format publish Visualize → Orion; server **đọc và expose đúng** contract, **proxy** control, **không** sửa producer.

---

## Cách test lại cho đúng 

1. `GET http://localhost:8080/api/system/health` → `controlApi` và `orion` đều `UP`.
2. `POST /api/auth/login` → session.
3. `POST /api/control/scenario` → `queued: true`.
4. Đợi vài giây → `GET /api/realtime/intersections/A` → `metadata.scenarioId` khớp, `consistent: true` (hoặc gọi lại nếu publish skew).

Chi tiết Postman: [`implementation/server_postman_guide.md`](implementation/server_postman_guide.md).

---

## Kết luận

**Trước :** Orion (và Visualize) đã publish đúng hướng Contract v1; **server** map thiếu field, **không** proxy scenario/control, **chưa** gom API và **chưa** có cách verify aggregate — nên cảm giác “server và Orion không khớp”.

**Sau :** Server align **read path** với golden contract, **control path** qua allowlist proxy, **một cổng `:8080`** cho Realtime quan sát + điều khiển; lệch còn lại nếu có thường do **timing** (`queued` → apply → publish) hoặc stack chưa chạy đủ (Visualize / Orion / PostgreSQL).
