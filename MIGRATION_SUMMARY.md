# SUMO Migration Summary

## 1. Mục đích

Tài liệu này mô tả ngắn gọn các thay đổi đã thực hiện khi chuyển hệ thống mô phỏng giao thông từ simulator Python cũ sang mô phỏng dựa trên SUMO.

Nhánh triển khai:

```text
feature/quoc
```

Nhánh này được dùng để lưu và kiểm tra toàn bộ phần migration trước khi xem xét tích hợp vào `main`.

---

## 2. Hệ thống trước migration

Phiên bản cũ sử dụng simulator Python tự xây dựng để sinh dữ liệu giao thông.

```text
Python Simulator
    ↓
Sinh dữ liệu giả lập
    ↓
Tạo dữ liệu VehicleSensor / TrafficLight / Intersection
    ↓
Gửi dữ liệu đến FIWARE Orion-LD
```

Hạn chế chính:

- Chuyển động của xe chưa được mô phỏng bằng mạng đường thực.
- Logic làn đường, nút giao và tín hiệu giao thông còn đơn giản.
- Khó trực quan hóa xe di chuyển và hàng chờ.
- Khó kiểm chứng dữ liệu theo trạng thái thực tế của từng phương tiện.
- Khả năng mở rộng kịch bản giao thông còn hạn chế.

---

## 3. Hệ thống sau migration

Hệ thống hiện tại sử dụng SUMO làm simulation engine và TraCI để đọc, điều khiển trạng thái mô phỏng.

```text
SUMO
    ↓ TraCI
Snapshot Provider
    ↓
Observation Layer
    ↓
Context / Metrics Processing
    ↓
NGSI-LD Entity Mapper
    ↓
FIWARE Orion-LD
```

Trong đó:

- **SUMO** mô phỏng mạng đường, làn xe, phương tiện và đèn giao thông.
- **TraCI** kết nối Python với SUMO theo từng simulation step.
- **Snapshot Provider** thu thập trạng thái hiện tại từ SUMO.
- **Observation Layer** chuẩn hóa dữ liệu giao thông.
- **Context Engine** xử lý trạng thái và các chỉ số liên quan.
- **Entity Mapper** ánh xạ dữ liệu nội bộ sang entity NGSI-LD.
- **Orion-LD** lưu context hiện tại và cung cấp dữ liệu cho các thành phần khác.

---

## 4. Các phần đã thực hiện

### 4.1. Tích hợp SUMO và TraCI

Đã xây dựng luồng khởi chạy SUMO từ Python và đọc dữ liệu theo từng bước mô phỏng.

Các dữ liệu chính được thu thập gồm:

- Thời gian mô phỏng.
- Số lượng phương tiện.
- Tốc độ trung bình.
- Chiều dài hàng chờ.
- Mức độ giao thông.
- Trạng thái đèn tín hiệu.
- Thông tin theo từng hướng North, South, East và West.

### 4.2. Xây dựng mạng giao thông SUMO

Đã bổ sung và cập nhật các tài nguyên SUMO:

- Node.
- Edge.
- Network.
- Route.
- Traffic light program.
- Detector.
- View settings.
- SUMO configuration.

Các file runtime chính nằm trong:

```text
Visualize/Visualize/
```

### 4.3. Tái cấu trúc source code

Các module đơn lẻ của phiên bản cũ đã được tách thành các package có trách nhiệm rõ ràng hơn:

```text
Visualize/
├── actuators/
├── api/
├── app/
├── configuration/
├── context/
├── context_engine/
├── integration/
├── observation/
├── runtime/
├── simulation/
├── topology/
└── tools/
```


### 4.4. Tích hợp Orion-LD

Đã xây dựng luồng tạo và cập nhật các entity NGSI-LD:

- `Intersection`
- `TrafficLight`
- `VehicleSensor`
- `Camera`

Đối với nút giao A, hệ thống hiện publish:

```text
1 Intersection
1 Camera
4 TrafficLight
4 VehicleSensor
```

Tổng cộng:

```text
10 entities
```

### 4.5. Ánh xạ hướng giao thông

Đã chuẩn hóa mapping giữa hướng giao thông, cạnh SUMO và entity Orion.

```text
North → NORTHBOUND → TrafficLight A-North
South → SOUTHBOUND → TrafficLight A-South
East  → EASTBOUND  → TrafficLight A-East
West  → WESTBOUND  → TrafficLight A-West
```

### 4.6. Thu thập và xử lý metric

Các metric cốt lõi hiện được lấy từ SUMO và publish lên Orion:

- `simulationTime`
- `vehicleCount`
- `averageSpeed`
- `queueLength`
- `trafficStatus`
- `occupancyRate`
- `pcuEquivalent`
- `dateObserved`
- trạng thái đèn giao thông


## 5. Kết quả kiểm thử hiện tại

Luồng end-to-end đã được kiểm tra:

```text
SUMO
    ↓
TraCI
    ↓
Snapshot
    ↓
Observation
    ↓
Mapper
    ↓
Orion-LD
```

Kết quả chính:

- Orion-LD hoạt động bình thường.
- Tạo entity thành công.
- Cập nhật entity bằng PATCH thành công.
- Không xuất hiện HTTP 400 hoặc HTTP 5xx trong stress test.
- Dữ liệu `simulationTime` và `vehicleCount` thay đổi theo thời gian thực.
- Mapping North, South, East và West đúng.
- Các metric cốt lõi từ Observation khớp với dữ liệu đọc lại từ Orion.
- Chạy được 300 chu kỳ publish liên tục.
- Pipeline realtime cốt lõi đã được xác minh hoạt động.
---

## 6. Cấu trúc source chính sau migration

```text
Smart_Data_Models/
├── Camera/
├── Intersection/
├── TrafficLight/
├── VehicleSensor/
├── context/
├── simulator/
├── docker-compose.yml
└── Visualize/
    ├── actuators/
    ├── api/
    ├── app/
    ├── configuration/
    ├── context/
    ├── context_engine/
    ├── generated/
    ├── integration/
    ├── observation/
    ├── runtime/
    ├── simulation/
    ├── topology/
    ├── tools/
    ├── Visualize/
    ├── config.py
    ├── control_api.py
    ├── model_params.py
    ├── traci_runner.py
    ├── requirements.txt
    ├── README.md
    ├── VERSION
    └── .env.example
```

---

## 7. Cách chạy cơ bản

### Khởi động Orion-LD và các service liên quan

```bash
docker compose up -d
```

### Cài dependency Python

```bash
cd Visualize
pip install -r requirements.txt
```

### Cấu hình môi trường

Tạo file `.env` dựa trên `.env.example`, sau đó cấu hình đúng `SUMO_HOME` và địa chỉ Orion-LD.

### Chạy mô phỏng

```bash
python traci_runner.py
```

---
---

## 9. Tóm tắt

Migration đã chuyển hệ thống từ một simulator Python tự sinh dữ liệu sang kiến trúc mô phỏng có SUMO làm simulation engine.

```text
Simulator Python cũ
        ↓
SUMO + TraCI + Observation + NGSI-LD Mapper + Orion-LD
```

Kết quả hiện tại là một realtime simulation pipeline có thể:

- Mô phỏng phương tiện và tín hiệu giao thông.
- Thu thập trạng thái theo từng bước mô phỏng.
- Chuẩn hóa dữ liệu theo từng hướng.
- Ánh xạ sang NGSI-LD.
- Publish và cập nhật dữ liệu realtime lên Orion-LD.
- Hỗ trợ mở rộng thêm dashboard, kịch bản điều khiển và Data Engineering ở các phase tiếp theo.
