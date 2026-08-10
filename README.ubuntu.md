# Smart Traffic System — Local demo (Ubuntu)

Tài liệu này mô tả cách chạy toàn bộ demo trên Ubuntu/Linux:

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
| Docker Engine + Compose v2 | Kafka, Orion, MongoDB, ClickHouse và DE processors | docker version |
| Git | lấy source | git --version |
| Java 21 (JDK) | Spring Server | java -version |
| PostgreSQL | database của Spring Server (traffic) | psql --version |
| Python 3.10+ | SUMO runner | python3 --version |
| Eclipse SUMO | mô phỏng và TraCI | kiểm tra SUMO_HOME |
| Node.js 20+ và pnpm (hoặc npm) | React/Vite Dashboard | node --version, pnpm --version |

Cài nhanh các package cơ bản trên Ubuntu:

~~~bash
sudo apt update
sudo apt install -y git openjdk-21-jdk postgresql postgresql-client python3 python3-venv python3-pip
# SUMO (từ PPA chính thức để có bản mới):
sudo add-apt-repository ppa:sumo/stable
sudo apt update
sudo apt install -y sumo sumo-tools sumo-doc
~~~

Maven không cần cài riêng: project có `server/mvnw`. Maven Wrapper cần Internet ở lần chạy đầu để tải Maven.

## 2. Lấy source và chuẩn bị PostgreSQL

~~~bash
git clone <repository-url>
cd Smart_Data_Models
~~~

docker-compose.yml hiện không tạo PostgreSQL. Spring mặc định kết nối:

~~~text
host=localhost
port=5432
database=traffic
user=erp_user
password=123456
~~~

Tạo database/user bằng psql (nếu đã tồn tại thì giữ nguyên):

~~~bash
sudo -u postgres psql -c "CREATE USER erp_user WITH PASSWORD '123456';"
sudo -u postgres psql -c "CREATE DATABASE traffic OWNER erp_user;"
~~~

Nếu PostgreSQL chạy bằng profile hoặc port khác, sửa biến môi trường/config local của Spring trước khi khởi động; không sửa docker-compose.yml để che lỗi kết nối.

## 3. Cài Python và SUMO

Đặt SUMO_HOME trỏ tới thư mục cài SUMO. Với bản cài từ apt trên Ubuntu, thường là `/usr/share/sumo`:

~~~bash
export SUMO_HOME="/usr/share/sumo"
export PATH="$SUMO_HOME/bin:$PATH"
sumo --version
~~~

Có thể thêm hai dòng export vào `~/.bashrc` để không phải đặt lại mỗi lần mở terminal.

Tạo môi trường Python cho producer:

~~~bash
cd Visualize
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
cd ..
~~~

Nếu muốn dùng đúng Python 3.12, thay `python3` bằng `python3.12` (cài qua `sudo apt install python3.12 python3.12-venv` nếu chưa có).

## 4. Cài Dashboard

~~~bash
cd frontend
corepack enable
pnpm install
cd ..
~~~

Có thể dùng `npm install` nếu không dùng pnpm. `frontend/.env.local` để trống VITE_API_BASE_URL là đúng cho local: Vite proxy /api tới http://localhost:8081 và giữ session cookie.

## 5. Khởi động hạ tầng Docker

Từ root repository:

~~~bash
docker compose up -d --build
docker compose ps
~~~

Lần đầu có thể cần vài chục giây vì Kafka có start_period 40 giây. Chờ tới khi kafka và clickhouse là healthy, de-migrate đã exited (0), sau đó Raw/Bronze/Silver/Gold/Projector mới được khởi động theo dependency.

Kiểm tra nhanh:

~~~bash
docker compose config --services
docker compose ps
curl -fsS http://localhost:8123/ping
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

Không chạy `docker compose down -v`, không xóa Kafka offset và không xóa `de/artifacts`. Hãy lưu log đầy đủ và chuyển volume cho người quản trị ClickHouse để detach/quarantine các part hỏng:

~~~bash
docker compose logs --no-color clickhouse > clickhouse-startup.log 2>&1
docker inspect de-clickhouse --format 'Status={{.State.Status}} ExitCode={{.State.ExitCode}} Health={{if .State.Health}}{{.State.Health.Status}}{{end}}'
~~~

Nếu dữ liệu trong volume là dữ liệu cần giữ, đây là nhánh phục hồi bắt buộc; không dùng nhánh reset demo bên dưới.

### 6.2. Tạo một demo sạch (MẤT dữ liệu ClickHouse hiện tại)

Chỉ dùng khi dữ liệu ClickHouse hiện tại có thể tái tạo từ Kafka và bạn chấp nhận mất toàn bộ Raw/Bronze/Silver/Gold trong volume. Trước hết dừng stack và xác nhận đúng project/volume:

~~~bash
docker compose down
docker compose ls
docker volume inspect smart_data_models_clickhouse-data
~~~

Backup state file trước khi reset (đường dẫn đích nằm trong repo):

~~~bash
stamp=$(date +%Y%m%d-%H%M%S)
backup="$(pwd)/artifacts/demo-backup-$stamp"
mkdir -p "$backup"
[ -d de/artifacts ] && mv de/artifacts "$backup/de-artifacts"
[ -d Visualize/artifacts ] && mv Visualize/artifacts "$backup/visualize-artifacts"
[ -d data/kafka ] && mv data/kafka "$backup/kafka"
mkdir -p de/artifacts Visualize/artifacts data/kafka
~~~

Sau khi kiểm tra backup, xóa đúng volume của project này rồi khởi động lại:

~~~bash
docker volume rm smart_data_models_clickhouse-data
docker compose up -d --build
docker compose ps
~~~

Không dùng `docker compose down -v`: lệnh đó có thể xóa cả MongoDB và các volume khác. Sau reset, cần chạy một SUMO run mới để Raw → Gold có dữ liệu lại.

## 7. Chạy SUMO và phát telemetry Kafka

Mở Terminal A, từ root repository:

~~~bash
cd Visualize
export ORION_PUBLISH_ENABLED="false"
export ORION_SYNC_PUBLISH="false"
export KAFKA_OUTBOX_ENABLED="true"
export KAFKA_PUBLISH_ENABLED="false"
export KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
export PYTHONUNBUFFERED="1"
.venv/bin/python -m app.traci_runner --gui --no-orion --nodes A,B,C,D --realtime --log-level INFO
~~~

Các biến trên buộc đường realtime dùng durable outbox → Kafka và không bật publish trực tiếp vào Orion. Giữ Terminal A chạy trong lúc xem demo; nhấn Ctrl+C để kết thúc run. Nếu không mở GUI, dùng --no-gui thay cho --gui.

## 8. Chạy Spring Server

Mở Terminal B:

~~~bash
cd server
export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"
export SPRING_PROFILES_ACTIVE="local,analytics"
# Trên Ubuntu/Linux: Orion (trong Docker) phải tải được JSON-LD context qua tên service nội bộ.
# host.docker.internal (mặc định trong application-local.properties cho Windows) thường timeout → Orion 503.
export ORION_CONTEXT_URL="http://context-provider/datamodels.context-ngsi.jsonld"
./mvnw spring-boot:run -Dspring-boot.run.profiles=local,analytics -Dspring-boot.run.arguments=--server.port=8081
~~~

Nếu không chắc đường dẫn JDK 21, kiểm tra bằng `sudo update-alternatives --list java` hoặc `ls /usr/lib/jvm/`.

Hoặc build JAR rồi chạy:

~~~bash
./mvnw -DskipTests package
export ORION_CONTEXT_URL="http://context-provider/datamodels.context-ngsi.jsonld"
java -jar target/server-0.0.1-SNAPSHOT.jar --spring.profiles.active=local,analytics --server.port=8081
~~~

Kiểm tra:

~~~bash
curl -fsS http://localhost:8081/api/system/health
~~~

Spring dùng PostgreSQL cho authentication/control và ClickHouse read-only cho Analytics; Analytics không làm realtime hoặc control phụ thuộc ClickHouse.

## 9. Chạy Dashboard

Mở Terminal C:

~~~bash
cd frontend
npm dev
~~~

Mở http://localhost:5173, đăng nhập local:

~~~text
username: admin
password: admin123
~~~

Vite proxy /api tới Spring ở port 8081. Không trỏ Dashboard trực tiếp tới Kafka, Orion, ClickHouse hoặc Python.

## 10. Xác nhận luồng từ SUMO tới Dashboard

### 10.1. Runtime health

~~~bash
urls=(
  "http://localhost:1026/version"
  "http://localhost:8123/ping"
  "http://localhost:8091/ready"
  "http://localhost:8092/ready"
  "http://localhost:8093/ready"
  "http://localhost:8095/ready"
  "http://localhost:8096/ready"
  "http://localhost:8081/api/system/health"
)
for url in "${urls[@]}"; do
  code=$(curl -s -o /dev/null -w "%{http_code}" "$url") \
    && echo "$url -> $code" \
    || echo "$url -> FAIL"
done
~~~

Projector ở trạng thái idle trước khi SUMO phát run là bình thường. Sau khi có RunStarted, /ready phải sẵn sàng, orion_apply_count tăng và lag giảm về 0.

### 10.2. Kiểm tra dữ liệu DE bằng ClickHouse

~~~bash
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM smart_traffic.kafka_raw_events"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM smart_traffic.bronze_entity_events"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM smart_traffic.silver_fact_traffic_observation"
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM smart_traffic.gold_fact_intersection_window"
~~~

Nếu Silver/Gold đang xử lý backlog, chờ vài phút rồi chạy lại; không reset checkpoint thủ công (cài `jq` bằng `sudo apt install -y jq` nếu chưa có):

~~~bash
curl -fsS http://localhost:8095/ready | jq .
curl -fsS http://localhost:8096/ready | jq .
~~~

### 10.3. Kiểm tra trên UI

* Realtime Overview/Intersections: số liệu đổi theo run mới, có simulation time và trạng thái Live.
* Intersection Detail: chọn A/B/C/D; signal và sensor lấy từ Orion current state.
* Analytics: window, comparison, trend, congestion, priority và signal operation đọc từ Gold qua Spring.
* GET /api/analytics/network/windows có thể trả 503 ANALYTICS_NOT_READY vì network mart hiện vẫn là scaffold WHERE 0; đây là gate của network mart, không phải lỗi realtime hay các mart intersection.

## 11. Chạy lại và dừng hệ thống

Normal restart không reset offset:

~~~bash
docker compose up -d
~~~

Projector dùng group cố định và SQLite ledger; không tạo group mới, không dùng --reset-offsets --to-latest, không xóa ledger để né backlog. Khi cần dừng:

~~~bash
docker compose stop
docker compose ps
~~~

`docker compose down` chỉ xóa container/network, không xóa named volume nếu không thêm -v.

## 12. Sự cố thường gặp

| Triệu chứng | Kiểm tra/xử lý |
|---|---|
| smart-traffic-kafka Waiting lâu | docker compose logs kafka --tail 100; Kafka có healthcheck 40 giây. Chờ tới healthy. |
| de-clickhouse dependency clickhouse failed | Đọc log ClickHouse; nếu thấy broken MergeTree parts như mục 6 thì chọn phục hồi hoặc reset demo có backup. |
| Raw/Bronze/Silver/Gold không tăng | Kiểm tra /ready từng service, Kafka topic và log processor; không tự đặt lại offset/checkpoint. |
| Frontend không có dữ liệu | Xác nhận Spring :8081, frontend :5173, frontend/.env.local có VITE_API_BASE_URL= rỗng và đã đăng nhập. |
| Dashboard báo Orion error 503 dù SUMO GUI vẫn chạy | SUMO GUI và Dashboard là hai luồng khác nhau. Dashboard đọc Orion qua Spring với JSON-LD context; trên Ubuntu cần export ORION_CONTEXT_URL=http://context-provider/datamodels.context-ngsi.jsonld trước khi khởi động Spring, rồi restart Spring. |
| release version 21 not supported | Đặt JAVA_HOME tới JDK 21 (export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64) rồi mở terminal mới. |
| SUMO không chạy | Kiểm tra SUMO_HOME, sumo --version, venv và Visualize/requirements.txt. |
| Permission denied khi ghi Visualize/artifacts | Container orion-projector mount ./Visualize/artifacts và có thể tạo thư mục thuộc root. Sửa: sudo chown -R $USER:$USER Visualize/artifacts rồi chạy lại traci_runner. |
| Docker build báo Permission denied trong de/artifacts | Dừng pytest/process đang giữ file, kiểm tra .dockerignore có loại de/artifacts/; không đưa runtime artifacts vào build context. |
| Analytics trả 503 | Phân biệt ANALYTICS_NOT_READY (mart chưa sẵn sàng/network scaffold) với ClickHouse unavailable; xem log Spring và /ready Gold. |
| Docker cần sudo | Thêm user vào group docker: sudo usermod -aG docker $USER rồi logout/login lại. |
| mvnw báo Permission denied | Cấp quyền thực thi: chmod +x server/mvnw. |
| Kafka Restarting liên tục (crash-loop) | Docker tự tạo data/kafka thuộc root nên Kafka (uid 1000) không ghi được. Sửa: sudo chown -R 1000:1000 data/kafka rồi docker compose up -d. |
| Daemon báo cannot stop container: permission denied | Profile AppArmor mồ côi sau khi nâng cấp Docker. Chạy sudo aa-remove-unknown rồi docker rm -f container kẹt. Lưu ý: aa-remove-unknown có thể gỡ nhầm profile của snap; nếu sau đó snap Docker không khởi động được (log báo missing profile snap.docker.dockerd) thì chạy sudo systemctl restart snapd.apparmor.service rồi khởi động lại Docker. |
| Container cùng mạng không kết nối được nhau (connect timeout) dù healthcheck pass | Kiểm tra có nhiều bản Docker chạy song song không: ps aux \| grep dockerd. Nếu thấy cả dockerd của snap lẫn /usr/bin/dockerd (docker-ce), hai daemon sẽ ghi đè iptables của nhau làm đứt mạng bridge. Tắt bản không dùng (ví dụ sudo systemctl disable --now docker.service docker.socket nếu container nằm trên snap) rồi restart daemon còn lại. Về lâu dài chỉ nên giữ một bản Docker. |

## 13. Tóm tắt lệnh theo thứ tự

~~~bash
# Terminal 1 — hạ tầng
docker compose up -d --build

# Terminal 2 — SUMO → Kafka
cd Visualize
export ORION_PUBLISH_ENABLED="false"
export ORION_SYNC_PUBLISH="false"
export KAFKA_OUTBOX_ENABLED="true"
export KAFKA_PUBLISH_ENABLED="false"
export KAFKA_BOOTSTRAP_SERVERS="localhost:29092"
.venv/bin/python -m app.traci_runner --gui --no-orion --nodes A,B,C,D --realtime

# Terminal 3 — Spring Server :8081
cd server
export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"
export ORION_CONTEXT_URL="http://context-provider/datamodels.context-ngsi.jsonld"
./mvnw spring-boot:run -Dspring-boot.run.profiles=local,analytics -Dspring-boot.run.arguments=--server.port=8081

# Terminal 4 — Dashboard :5173
cd frontend
pnpm dev
~~~

Khi bốn terminal đang chạy, đường kiểm chứng là:

~~~text
SUMO → Kafka → Projector → Orion → Spring → Dashboard
             └→ Raw → Bronze → Silver → Gold → Spring Analytics → Dashboard
~~~

## Phụ lục — Lệnh nhanh (tương đương phần cuối README gốc)

~~~bash
cd Smart_Data_Models

docker compose up -d
docker compose ps
docker compose stop
~~~

~~~bash
export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"
export PATH="$JAVA_HOME/bin:$PATH"
export SPRING_PROFILES_ACTIVE="local,analytics"
export SERVER_PORT="8081"
export ORION_CONTEXT_URL="http://context-provider/datamodels.context-ngsi.jsonld"
export MAVEN_OPTS="-Xmx384m -XX:MaxMetaspaceSize=192m"

cd server
./mvnw spring-boot:run
~~~

~~~bash
cd Visualize

export SUMO_HOME="/usr/share/sumo"
export ORION_PUBLISH_ENABLED="false"
export ORION_SYNC_PUBLISH="false"
export KAFKA_OUTBOX_ENABLED="true"
export KAFKA_PUBLISH_ENABLED="false"
export KAFKA_BOOTSTRAP_SERVERS="localhost:29092"

.venv/bin/python -m app.traci_runner \
  --gui \
  --no-orion \
  --nodes A,B,C,D \
  --realtime \
  --publish-interval 5 \
  --log-level INFO
~~~

~~~bash
cd frontend

npm run dev
~~~
