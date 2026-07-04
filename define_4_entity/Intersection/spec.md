# Giao lộ (Intersection) - Đặc tả Smart Data Model

## 1. Mô tả tổng quan
Thực thể `Intersection` đại diện cho một ngã tư trong hệ thống điều hành giao thông thông minh. Nó đóng vai trò là "Nút trung tâm" (Hub Entity), nơi tổng hợp dữ liệu vĩ mô từ các thiết bị vi mô (VehicleSensor, TrafficLight, Camera) để đánh giá sức khỏe toàn diện của nút giao đó.

## 2. Đặc tả Thuộc tính (Properties)

### Định danh và Vị trí
* **`id`** *(Bắt buộc)*: String (URN). Khóa chính của giao lộ. Ví dụ: `urn:ngsi-ld:Intersection:A`.
* **`type`** *(Bắt buộc)*: String. Luôn là `Intersection`.
* **`name`** *(Bắt buộc)*: String. Tên định danh (Ví dụ: "Nguyen Hue - Le Loi").
* **`location`** *(Bắt buộc)*: GeoJSON Point. Tọa độ địa lý `[longitude, latitude]` phục vụ hiển thị trên bản đồ số và truy vấn không gian.
* **`status`** *(Tùy chọn)*: String (`ACTIVE`, `INACTIVE`, `MAINTENANCE`). Trạng thái vòng đời của nút giao.

### Chỉ số Giao thông Vĩ mô (Macro Traffic Metrics)
*Các thông số này là kết quả tổng hợp (Aggregation) từ 4 hướng của VehicleSensor.*
* **`totalVehicleCount`** *(Tùy chọn)*: Integer (>= 0). Tổng lượng xe tại tất cả các hướng.
* **`averageSpeed`** *(Tùy chọn)*: Number (>= 0). Vận tốc trung bình (km/h) của toàn ngã tư.
* **`totalQueueLength`** *(Tùy chọn)*: Number (>= 0). Tổng chiều dài hàng đợi (mét) của cả 4 hướng cộng lại.
* **`density`** *(Tùy chọn)*: String (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`). Nhãn cảnh báo mức độ ùn tắc chung.

### Máy Trạng Thái Pha Đèn (Phase State Machine)
*Quản lý chu kỳ hoạt động tổng thể, làm căn cứ để các TrafficLight đơn lẻ đồng bộ màu sắc.*
* **`currentPhase`** *(Tùy chọn)*: String. Tên pha điều khiển đang được kích hoạt (Ví dụ: `NS_GREEN` - Hướng Bắc Nam đèn xanh).
* **`nextPhase`** *(Tùy chọn)*: String. Tên pha tiếp theo trong chu kỳ (Ví dụ: `NS_YELLOW`).
* **`phaseRemainingSeconds`** *(Tùy chọn)*: Integer (>= 0). Số giây còn lại trước khi chuyển sang `nextPhase`.
* **`phaseDuration`** *(Tùy chọn)*: Integer (>= 0). Tổng thời lượng của `currentPhase`. 

### Quản lý Sự cố & Ngữ cảnh (Incident & Context)
* **`incidentDetected`** *(Tùy chọn)*: Boolean. Cờ báo hiệu ngã tư đang có sự cố (dựa trên Camera hoặc thuật toán Anomaly Detection).
* **`incidentCount`** *(Tùy chọn)*: Integer (>= 0). Số lượng sự cố.
* **`incidentDirection`** *(Tùy chọn)*: String. Hướng đang chịu ảnh hưởng của sự cố (`North`, `South`, `East`, `West`, hoặc `NONE`). Giúp Dashboard nhấp nháy đỏ đúng hướng.
* **`scenario`** *(Tùy chọn)*: String. Kịch bản mô phỏng hoặc ngữ cảnh môi trường đang áp dụng tại giao lộ (Ví dụ: `normal`, `morning_peak`, `rain`, `accident`).
* **`lastUpdate`** *(Tùy chọn)*: DateTime. Dấu thời gian hệ thống chốt số liệu vòng lặp gần nhất.

## 3. Mối quan hệ (Relationships)
Bản thân `Intersection` là thực thể gốc (Root Entity) nên nó không trỏ Relationship lên thực thể nào khác. Ngược lại, tất cả các thực thể khác (Camera, TrafficLight, VehicleSensor) phải dùng thuộc tính `refIntersection` trỏ về nó.