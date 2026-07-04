# Cảm biến Phương tiện (VehicleSensor) - Đặc tả Smart Data Model

## 1. Mô tả tổng quan
Thực thể `VehicleSensor` mô phỏng một trạm thu thập dữ liệu giao thông cục bộ (có thể là vòng từ trường Loop Detector hoặc Radar) đặt tại một hướng tiếp cận (Approach) của ngã tư. Nó đo lường và cung cấp các chỉ số giao thông vi mô làm đầu vào cho bài toán đánh giá ùn tắc (Congestion Score) và phát hiện sự cố (Incident Detection) ở tầng Data Warehouse.

## 2. Đặc tả Thuộc tính (Properties)

### Định danh và Vị trí
* **`id`** *(Bắt buộc)*: String (URN). Khuyến nghị cú pháp nối ghép để định vị.
  * *Ví dụ:* `urn:ngsi-ld:VehicleSensor:A-North`
* **`type`** *(Bắt buộc)*: String. Luôn là `VehicleSensor`.
* **`direction`** *(Bắt buộc)*: String. Hướng của dòng xe tiến vào giao lộ. 
  * *Giá trị:* `North`, `South`, `East`, `West`.

### Chỉ số Lưu lượng & Phân luồng (Traffic Flow Metrics)
* **`vehicleCount`** *(Tùy chọn)*: Integer (>= 0). Tổng số phương tiện được ghi nhận.
* **`leftTurnCount` / `straightCount` / `rightTurnCount`** *(Tùy chọn)*: Integer (>= 0). Số lượng xe phân bổ theo quỹ đạo dự kiến.
  * ⚠️ **Ràng buộc Toàn vẹn Logic (Data Integrity):** Hệ thống Simulator phải đảm bảo phương trình sau luôn đúng tại mọi thời điểm cập nhật:
    `vehicleCount = leftTurnCount + straightCount + rightTurnCount`
    *(Sự sai lệch của phương trình này sẽ bị tầng Data Pipeline coi là dữ liệu dị thường - Anomaly Data).*

### Chỉ số Tắc nghẽn & Động lực học (Congestion & Dynamics Metrics)
* **`averageSpeed`** *(Tùy chọn)*: Number (>= 0). Vận tốc trung bình (km/h) của dòng xe. Khi giá trị này giảm đột ngột về dưới 15km/h, hệ thống sẽ trigger cảnh báo `SPEED_DROP`.
* **`waitingVehicleCount`** *(Tùy chọn)*: Integer (>= 0). Trích xuất từ `vehicleCount` nhưng chỉ đếm các xe có vận tốc xấp xỉ 0 (đang dừng chờ đèn đỏ hoặc kẹt cứng).
* **`queueLength`** *(Tùy chọn)*: Number (>= 0). Chiều dài hàng đợi ước tính bằng mét. Đây là tham số có **trọng số cao nhất (35%)** trong thuật toán tính `Congestion Score`.
* **`occupancy`** *(Tùy chọn)*: Number (0 - 100). Tỷ lệ phần trăm diện tích mặt đường bị phương tiện chiếm dụng trên tổng diện tích quan sát của cảm biến.
* **`density`** *(Tùy chọn)*: String (`LOW`, `MEDIUM`, `HIGH`). Nhãn phân loại mức độ đông đúc mang tính định tính, phục vụ hiển thị nhanh trên Real-time Dashboard.

## 3. Mối quan hệ (Relationships)
* **`refIntersection`** *(Bắt buộc)*: Relationship (URI). Khóa ngoại trỏ về Giao lộ (Intersection) tương ứng. Dùng để thực hiện phép JOIN trong ClickHouse khi cần tổng hợp dữ liệu 4 hướng lại thành thông số vĩ mô của toàn ngã tư.