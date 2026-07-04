# Camera Giám sát (Camera) - Đặc tả Smart Data Model

## 1. Mô tả tổng quan
Thực thể `Camera` đại diện cho một thiết bị giám sát quang học được tích hợp mô hình Computer Vision (AI). Nhiệm vụ của nó là quét toàn cảnh giao lộ (Intersection) để cung cấp số liệu bao quát và đặc biệt là phát hiện các sự cố bất thường (tai nạn, chắn làn) độc lập với dữ liệu của cảm biến từ trường/radar.

## 2. Đặc tả Thuộc tính (Properties)

### Định danh và Kết nối
* **`id`** *(Bắt buộc)*: String (URN). Ví dụ: `urn:ngsi-ld:Camera:A`
* **`type`** *(Bắt buộc)*: String. Luôn là `Camera`.
* **`cameraStatus`** *(Tùy chọn)*: String (`ONLINE`, `OFFLINE`, `ERROR`). Trạng thái sức khỏe (Health Check) của luồng truyền phát video.

### Chỉ số Nhận diện (AI Vision Metrics)
* **`vehicleCount`** *(Tùy chọn)*: Integer (>= 0). Tổng số bounding box (hộp giới hạn) nhận diện là phương tiện trong khung hình hiện tại.

### Cảnh báo Sự cố (Incident Detection)
*Đây là nhóm thuộc tính cốt lõi tạo nên sự "thông minh" của thực thể này, đóng vai trò Trigger cảnh báo khẩn cấp lên Dashboard.*
* **`incidentDetected`** *(Tùy chọn)*: Boolean. Cờ báo động đỏ. Nếu `true`, hệ thống sẽ kích hoạt luồng xử lý ưu tiên.
* **`incidentCount`** *(Tùy chọn)*: Integer (>= 0). Tổng số sự kiện bất thường. Ràng buộc logic: `incidentCount = minorAccidentCount + laneBlockedCount + roadClosedCount`.
* **`minorAccidentCount`** *(Tùy chọn)*: Integer. Số vụ va chạm giao thông.
* **`laneBlockedCount`** *(Tùy chọn)*: Integer. Số làn đường bị cản trở (do xe hỏng, vật cản).
* **`roadClosedCount`** *(Tùy chọn)*: Integer. Số hướng đi đang bị phong tỏa.

## 3. Mối quan hệ (Relationships)
* **`refIntersection`** *(Bắt buộc)*: Relationship (URI). Định danh của Giao lộ mà Camera đang quan sát. Trọng tâm thiết kế: Một Giao lộ có thể có nhiều Cảm biến (VehicleSensor) nhưng thường chỉ cần 1 Camera quang học góc rộng trên cao.