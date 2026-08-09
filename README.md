# Smart Traffic System — Local demo

Tài liệu này mô tả cách chạy toàn bộ demo trên Windows:

~~~text
SUMO/TraCI → Python snapshot + durable outbox → Kafka
  → Orion Projector → FIWARE Orion → Spring Server → React Dashboard

Kafka → Raw v2 → Bronze → Silver → Gold marts
  → Spring Analytics API → React Dashboard
~~~

Dashboard chỉ gọi Spring Server. SUMO không gửi command vào Kafka; Kafka chỉ là telemetry/event backbone. Projector giữ current state trong Orion, còn lịch sử được xử lý ở nhánh Raw/Bronze/Silver/Gold.

## 1. Cần cài trước

| Thành phần | Mục đích | Kiểm tra |
|---|---|---|
| Docker Desktop + Compose v2 | Kafka, Orion, MongoDB, ClickHouse và DE processors | docker version |
| Git | lấy source | git --version |
| Java 21 (JDK) | Spring Server | java -version |
| PostgreSQL | database của Spring Server (traffic) | psql --version |
| Python 3.10+ | SUMO runner | py --version |
| Eclipse SUMO | mô phỏng và TraCI | kiểm tra SUMO_HOME |
| Node.js 20+ và pnpm (hoặc npm) | React/Vite Dashboard | node --version, pnpm --version |

Maven không cần cài riêng: project có server\mvnw.cmd. Maven Wrapper cần Internet ở lần chạy đầu để tải Maven.

## 2. Lấy source và chuẩn bị PostgreSQL

~~~powershell
git clone <repository-url>
Set-Location Smart_Data_Models
~~~

docker-compose.yml hiện không tạo PostgreSQL. Spring mặc định kết nối:

~~~text
host=localhost
port=5432
database=traffic
user=erp_user
password=123456
~~~

Tạo database/user bằng psql hoặc pgAdmin (nếu đã tồn tại thì giữ nguyên):

~~~powershell
psql -U postgres -c "CREATE USER erp_user WITH PASSWORD '123456';"
psql -U postgres -c "CREATE DATABASE traffic OWNER erp_user;"
~~~

Nếu PostgreSQL chạy bằng profile hoặc port khác, sửa biến môi trường/config local của Spring trước khi khởi động; không sửa docker-compose.yml để che lỗi kết nối.

## 3. Cài Python và SUMO

Đặt SUMO_HOME trỏ tới thư mục cài SUMO. Ví dụ:

~~~powershell
$env:SUMO_HOME = "C:\Program Files (x86)\Eclipse\Sumo"
$env:Path = "$env:SUMO_HOME\bin;$env:Path"
sumo --version
~~~

Tạo môi trường Python cho producer:

~~~powershell
Set-Location Visualize
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Set-Location ..
~~~

Nếu Python của máy là phiên bản khác, thay py -3.12 bằng phiên bản thực tế.

## 4. Cài Dashboard

~~~powershell
Set-Location frontend
corepack enable
pnpm install
Set-Location ..
~~~

Có thể dùng npm install nếu không dùng pnpm. frontend/.env.local để trống VITE_API_BASE_URL là đúng cho local: Vite proxy /api tới http://localhost:8081 và giữ session cookie.

## 5. Khởi động hạ tầng Docker

Từ root repository:

~~~powershell
docker compose up -d --build
docker compose ps
~~~

Lần đầu có thể cần vài chục giây vì Kafka có start_period 40 giây. Chờ tới khi kafka và clickhouse là healthy, de-migrate đã exited (0), sau đó Raw/Bronze/Silver/Gold/Projector mới được khởi động theo dependency.

Kiểm tra nhanh:

~~~powershell
docker compose config --services
docker compose ps
Invoke-WebRequest http://localhost:8123/ping -UseBasicParsing
docker compose logs --no-color --tail 80 kafka
docker compose logs --no-color --tail 80 clickhouse
~~~

Default runtime gồm Kafka, Orion, MongoDB, Context server, ClickHouse, de-migrate, Raw consumer, Bronze, Silver, Gold và Orion Projector. Webhook không được tạo trong default runtime.

## 6. Lỗi ClickHouse hiện tại và cách xử lý

Thông báo:

~~~text
de-clickhouse: dependency clickhouse failed to start
~~~

không phải lỗi Kafka. Log thực tế của container hiện tại cho biết:

~~~text
CANNOT_PARSE_INPUT_ASSERTION_FAILED
TOO_MANY_UNEXPECTED_DATA_PARTS
broken part ... silver_processing_ledger
broken part ... silver_fact_signal_state
ExitCode=183, OOMKilled=false
~~~

Named volume smart_data_models_clickhouse-data còn các part MergeTree bị hỏng (thường do container bị dừng giữa lúc ghi). ClickHouse không thể attach bảng Silver nên healthcheck fail; các processor phụ thuộc vào ClickHouse cũng không thể chạy. Dòng smart-traffic-kafka Waiting chỉ là Kafka đang chờ healthcheck riêng, không phải nguyên nhân của exit 183.

### 6.1. Giữ dữ liệu hiện tại

Không chạy docker compose down -v, không xóa Kafka offset và không xóa de\artifacts. Hãy lưu log đầy đủ và chuyển volume cho người quản trị ClickHouse để detach/quarantine các part hỏng:

~~~powershell
docker compose logs --no-color clickhouse *> clickhouse-startup.log
docker inspect de-clickhouse --format "Status={{.State.Status}} ExitCode={{.State.ExitCode}} Health={{if .State.Health}}{{.State.Health.Status}}{{end}}"
~~~

Nếu dữ liệu trong volume là dữ liệu cần giữ, đây là nhánh phục hồi bắt buộc; không dùng nhánh reset demo bên dưới.

### 6.2. Tạo một demo sạch (MẤT dữ liệu ClickHouse hiện tại)

Chỉ dùng khi dữ liệu ClickHouse hiện tại có thể tái tạo từ Kafka và bạn chấp nhận mất toàn bộ Raw/Bronze/Silver/Gold trong volume. Trước hết dừng stack và xác nhận đúng project/volume:

~~~powershell
docker compose down
docker compose ls
docker volume inspect smart_data_models_clickhouse-data
~~~

Backup state file trước khi reset (đường dẫn đích nằm trong repo):

~~~powershell
$stamp = Get-Date -Format yyyyMMdd-HHmmss
$backup = Join-Path (Get-Location) "artifacts\demo-backup-$stamp"
New-Item -ItemType Directory -Force $backup | Out-Null
if (Test-Path de\artifacts) { Move-Item de\artifacts (Join-Path $backup "de-artifacts") }
if (Test-Path Visualize\artifacts) { Move-Item Visualize\artifacts (Join-Path $backup "visualize-artifacts") }
if (Test-Path data\kafka) { Move-Item data\kafka (Join-Path $backup "kafka") }
New-Item -ItemType Directory -Force de\artifacts,Visualize\artifacts,data\kafka | Out-Null
~~~

Sau khi kiểm tra backup, xóa đúng volume của project này rồi khởi động lại:

~~~powershell
docker volume rm smart_data_models_clickhouse-data
docker compose up -d --build
docker compose ps
~~~

Không dùng docker compose down -v: lệnh đó có thể xóa cả MongoDB và các volume khác. Sau reset, cần chạy một SUMO run mới để Raw → Gold có dữ liệu lại.

## 7. Chạy SUMO và phát telemetry Kafka

Mở Terminal A, từ root repository:

~~~powershell
Set-Location Visualize
$env:ORION_PUBLISH_ENABLED = "false"
$env:ORION_SYNC_PUBLISH = "false"
$env:KAFKA_OUTBOX_ENABLED = "true"
$env:KAFKA_PUBLISH_ENABLED = "false"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
$env:PYTHONUNBUFFERED = "1"
.\.venv\Scripts\python.exe -m app.traci_runner --gui --no-orion --nodes A,B,C,D --realtime --log-level INFO
~~~

Các biến trên buộc đường realtime dùng durable outbox → Kafka và không bật publish trực tiếp vào Orion. Giữ Terminal A chạy trong lúc xem demo; nhấn Ctrl+C để kết thúc run. Nếu không mở GUI, dùng --no-gui thay cho --gui.

## 8. Chạy Spring Server

Mở Terminal B:

~~~powershell
Set-Location server
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
$env:SPRING_PROFILES_ACTIVE = "local,analytics"
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=local,analytics" "-Dspring-boot.run.arguments=--server.port=8081"
~~~

Hoặc build JAR rồi chạy:

~~~powershell
.\mvnw.cmd -DskipTests package
java -jar target\server-0.0.1-SNAPSHOT.jar --spring.profiles.active=local,analytics --server.port=8081
~~~

Kiểm tra:

~~~powershell
Invoke-RestMethod http://localhost:8081/api/system/health
~~~

Spring dùng PostgreSQL cho authentication/control và ClickHouse read-only cho Analytics; Analytics không làm realtime hoặc control phụ thuộc ClickHouse.

## 9. Chạy Dashboard

Mở Terminal C:

~~~powershell
Set-Location frontend
pnpm dev
~~~

Mở http://localhost:5173, đăng nhập local:

~~~text
username: admin
password: admin123
~~~

Vite proxy /api tới Spring ở port 8081. Không trỏ Dashboard trực tiếp tới Kafka, Orion, ClickHouse hoặc Python.

## 10. Xác nhận luồng từ SUMO tới Dashboard

### 10.1. Runtime health

~~~powershell
$urls = @(
  "http://localhost:1026/version",
  "http://localhost:8123/ping",
  "http://localhost:8091/ready",
  "http://localhost:8092/ready",
  "http://localhost:8093/ready",
  "http://localhost:8095/ready",
  "http://localhost:8096/ready",
  "http://localhost:8081/api/system/health"
)
foreach ($url in $urls) {
  try { "$url -> $((Invoke-WebRequest $url -UseBasicParsing).StatusCode)" }
  catch { "$url -> FAIL: $($_.Exception.Message)" }
}
~~~

Projector ở trạng thái idle trước khi SUMO phát run là bình thường. Sau khi có RunStarted, /ready phải sẵn sàng, orion_apply_count tăng và lag giảm về 0.

### 10.2. Kiểm tra dữ liệu DE bằng ClickHouse

~~~powershell
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM smart_traffic.kafka_raw_events"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM smart_traffic.bronze_entity_events"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM smart_traffic.silver_fact_traffic_observation"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM smart_traffic.gold_fact_intersection_window"
~~~

Nếu Silver/Gold đang xử lý backlog, chờ vài phút rồi chạy lại; không reset checkpoint thủ công:

~~~powershell
Invoke-RestMethod http://localhost:8095/ready | ConvertTo-Json -Depth 8
Invoke-RestMethod http://localhost:8096/ready | ConvertTo-Json -Depth 8
~~~

### 10.3. Kiểm tra trên UI

* Realtime Overview/Intersections: số liệu đổi theo run mới, có simulation time và trạng thái Live.
* Intersection Detail: chọn A/B/C/D; signal và sensor lấy từ Orion current state.
* Analytics: window, comparison, trend, congestion, priority và signal operation đọc từ Gold qua Spring.
* GET /api/analytics/network/windows có thể trả 503 ANALYTICS_NOT_READY vì network mart hiện vẫn là scaffold WHERE 0; đây là gate của network mart, không phải lỗi realtime hay các mart intersection.

## 11. Chạy lại và dừng hệ thống

Normal restart không reset offset:

~~~powershell
docker compose up -d
~~~

Projector dùng group cố định và SQLite ledger; không tạo group mới, không dùng --reset-offsets --to-latest, không xóa ledger để né backlog. Khi cần dừng:

~~~powershell
docker compose stop
docker compose ps
~~~

docker compose down chỉ xóa container/network, không xóa named volume nếu không thêm -v.

## 12. Sự cố thường gặp

| Triệu chứng | Kiểm tra/xử lý |
|---|---|
| smart-traffic-kafka Waiting lâu | docker compose logs kafka --tail 100; Kafka có healthcheck 40 giây. Chờ tới healthy. |
| de-clickhouse dependency clickhouse failed | Đọc log ClickHouse; nếu thấy broken MergeTree parts như mục 6 thì chọn phục hồi hoặc reset demo có backup. |
| Raw/Bronze/Silver/Gold không tăng | Kiểm tra /ready từng service, Kafka topic và log processor; không tự đặt lại offset/checkpoint. |
| Frontend không có dữ liệu | Xác nhận Spring :8081, frontend :5173, frontend/.env.local có VITE_API_BASE_URL= rỗng và đã đăng nhập. |
| release version 21 not supported | Đặt JAVA_HOME tới JDK 21 rồi mở terminal mới. |
| SUMO không chạy | Kiểm tra SUMO_HOME, sumo --version, venv và Visualize\requirements.txt. |
| Docker build báo Access is denied trong de\artifacts | Dừng pytest/process đang giữ file, kiểm tra .dockerignore có loại de/artifacts/; không đưa runtime artifacts vào build context. |
| Analytics trả 503 | Phân biệt ANALYTICS_NOT_READY (mart chưa sẵn sàng/network scaffold) với ClickHouse unavailable; xem log Spring và /ready Gold. |

## 13. Tóm tắt lệnh theo thứ tự

~~~powershell
# Terminal 1 — hạ tầng
docker compose up -d --build

# Terminal 2 — SUMO → Kafka
Set-Location Visualize
$env:ORION_PUBLISH_ENABLED="false"
$env:ORION_SYNC_PUBLISH="false"
$env:KAFKA_OUTBOX_ENABLED="true"
$env:KAFKA_PUBLISH_ENABLED="false"
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
.\.venv\Scripts\python.exe -m app.traci_runner --gui --no-orion --nodes A,B,C,D --realtime

# Terminal 3 — Spring Server :8081
Set-Location server
$env:JAVA_HOME="C:\Program Files\Java\jdk-21"
.\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=local,analytics" "-Dspring-boot.run.arguments=--server.port=8081"

# Terminal 4 — Dashboard :5173
Set-Location frontend
pnpm dev
~~~

Khi bốn terminal đang chạy, đường kiểm chứng là:

~~~text
SUMO → Kafka → Projector → Orion → Spring → Dashboard
             └→ Raw → Bronze → Silver → Gold → Spring Analytics → Dashboard
~~~




cd  Smart_Data_Models

docker compose up -d
docker compose ps
docker compose stop
=========

$env:JAVA_HOME="C:\Program Files\Java\jdk-21"
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
$env:SPRING_PROFILES_ACTIVE="local,analytics"
$env:SERVER_PORT="8081"
$env:MAVEN_OPTS="-Xmx384m -XX:MaxMetaspaceSize=192m"

cd server
.\mvnw.cmd spring-boot:run

==========
cd Visualize

$env:SUMO_HOME="D:\SUMO"
$env:ORION_PUBLISH_ENABLED="false"
$env:ORION_SYNC_PUBLISH="false"
$env:KAFKA_OUTBOX_ENABLED="true"
$env:KAFKA_PUBLISH_ENABLED="false"
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"

..\.venv\Scripts\python.exe -m app.traci_runner `
  --gui `
  --no-orion `
  --nodes A,B,C,D `
  --realtime `
  --publish-interval 5 `
  --log-level INFO

====
cd frontend

npm.cmd run dev

## Cài đặt và chạy trên máy mới sau khi clone

Lỗi `ModuleNotFoundError: No module named 'yaml'` nghĩa là virtualenv chưa cài
PyYAML. Hãy cài toàn bộ dependency vào đúng virtualenv đang chạy.

### Phần mềm bắt buộc

Cần có Git, Docker Desktop (Compose v2), Python 3.12, Eclipse SUMO, JDK 21,
PostgreSQL và Node.js 20+ (khuyến nghị Node 22 LTS).

```powershell
git --version
docker version
py --version
java -version
node --version
psql --version
```

### Clone và PostgreSQL

```powershell
cd D:\
git clone <URL_REPOSITORY>
cd DO_AN_TTTN_v0
```

`docker-compose.yml` không tạo PostgreSQL. Spring mặc định dùng
`localhost:5432/traffic`, user `erp_user`, password `123456`:

```powershell
psql -U postgres -c "CREATE USER erp_user WITH PASSWORD '123456';"
psql -U postgres -c "CREATE DATABASE traffic OWNER erp_user;"
```

Nếu user hoặc database đã tồn tại thì giữ nguyên.

### SUMO và Python

Thay đường dẫn SUMO nếu cần:

```powershell
$env:SUMO_HOME="C:\Program Files (x86)\Eclipse\Sumo"
$env:PATH="$env:SUMO_HOME\bin;$env:PATH"
sumo-gui --version
```

Tạo virtualenv trong `Visualize\.venv`:

```powershell
cd D:\DO_AN_TTTN_v0\Visualize
py -3.12 -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Kiểm tra:

```powershell
python -c "import sys; print(sys.executable)"
python -m pip -V
python -c "import yaml, traci, fastapi, confluent_kafka; print('Python dependencies OK')"
```

Nếu cần sửa nhanh lỗi trong hình:

```powershell
python -m pip install PyYAML
```

### Frontend

```powershell
cd D:\DO_AN_TTTN_v0\frontend
npm.cmd ci
```

Nếu `npm ci` lỗi do lockfile, dùng `npm.cmd install`.

### Docker stack

```powershell
cd D:\DO_AN_TTTN_v0
docker compose up -d --build
docker compose ps
```

Lần đầu Kafka/ClickHouse có thể mất 30–60 giây. Kiểm tra:

```powershell
Invoke-RestMethod http://localhost:8091/ready
Invoke-RestMethod http://localhost:8092/ready
Invoke-RestMethod http://localhost:8095/ready
Invoke-RestMethod http://localhost:8096/ready
Invoke-RestMethod http://localhost:8093/ready
```

HTTP 503 trong lúc warm-up là bình thường; không reset Kafka offset.

### Spring Server

Mở PowerShell riêng:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-21"
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
$env:SPRING_PROFILES_ACTIVE="local,analytics"
$env:SERVER_PORT="8081"
$env:MAVEN_OPTS="-Xmx384m -XX:MaxMetaspaceSize=192m"

cd D:\DO_AN_TTTN_v0\server
.\mvnw.cmd spring-boot:run
```

### SUMO realtime liên tục

Mở PowerShell riêng và chạy foreground để GUI/TraCI dùng cùng một phiên:

```powershell
cd D:\DO_AN_TTTN_v0\Visualize
$env:SUMO_HOME="C:\Program Files (x86)\Eclipse\Sumo"
$env:PATH="$env:SUMO_HOME\bin;$env:PATH"
$env:ORION_PUBLISH_ENABLED="false"
$env:ORION_SYNC_PUBLISH="false"
$env:KAFKA_OUTBOX_ENABLED="true"
$env:KAFKA_PUBLISH_ENABLED="false"
$env:KAFKA_BOOTSTRAP_SERVERS="localhost:29092"

python -m app.traci_runner `
  --gui `
  --no-orion `
  --nodes A,B,C,D `
  --realtime `
  --publish-interval 5 `
  --log-level INFO
```

Không thêm `--max-sim-time` nếu muốn chạy liên tục. Cadence 5 giây giúp máy
local không bị Projector vượt lag.

### Dashboard

Mở PowerShell riêng:

```powershell
cd D:\DO_AN_TTTN_v0\frontend
npm.cmd run dev -- --host 127.0.0.1 --port 5173
```

Mở `http://localhost:5173`, đăng nhập `admin` / `admin123`.

Nếu vẫn gặp `No module named 'yaml'`, `python -m pip -V` phải trỏ vào
`DO_AN_TTTN_v0\Visualize\.venv`; chạy lại `python -m pip install -r
requirements.txt` bằng chính interpreter đó.

Không chạy `docker compose down -v`, `docker volume prune` hoặc xóa SQLite
outbox/Projector nếu muốn giữ dữ liệu và offset cho lần demo sau.
