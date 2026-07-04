# Đèn tín hiệu (TrafficLight) - Đặc tả Smart Data Model

## 1. Mô tả tổng quan
Thực thể `TrafficLight` đại diện cho một cụm đèn tín hiệu giao thông (Signal Head) chịu trách nhiệm điều khiển luồng phương tiện tại một hướng cụ thể của ngã tư.
Khác với `Intersection` mang tính chất vĩ mô, `TrafficLight` là thực thể vi mô, phản ánh trực tiếp trạng thái vật lý của bóng đèn và đồng hồ đếm ngược tại thời điểm hiện tại. Nó nhận lệnh điều khiển thông qua State Machine của Simulator hoặc lệnh can thiệp thủ công từ Dashboard.

## 2. Đặc tả Thuộc tính (Properties)

### Định danh và Phân loại
* **`id`** *(Bắt buộc)*: String (URN). Định danh duy nhất. Khuyến nghị sử dụng cú pháp nối ghép để dễ truy xuất.
  * *Ví dụ:* `urn:ngsi-ld:TrafficLight:A-North`
* **`type`** *(Bắt buộc)*: String. Luôn luôn có giá trị là `TrafficLight`.
* **`direction`** *(Bắt buộc)*: String. Hướng tiếp cận mà đèn này đang quay mặt về để điều khiển luồng xe.
  * *Giá trị hợp lệ:* `North`, `South`, `East`, `West`.

### Trạng thái Thời gian thực (Real-time State)
* **`color`** *(Tùy chọn)*: String. Trạng thái vật lý của bóng đèn ngay lúc này. 
  * *Giá trị hợp lệ:* `red`, `green`, `yellow`.
  * *Lưu ý nghiệp vụ:* Không bao giờ có 2 cụm đèn hướng vuông góc (ví dụ North và East) cùng mang giá trị `green` tại một thời điểm (để tránh xung đột va chạm).
* **`remainingSeconds`** *(Tùy chọn)*: Integer (>= 0). Đồng hồ đếm ngược của màu đèn hiện tại. Khi giá trị này chạm `0`, trường `color` phải được chuyển đổi ở chu kỳ mô phỏng tiếp theo.

### Cấu hình Pha & Chu kỳ (Cycle Configuration)
* **`currentPhase`** *(Tùy chọn)*: String. Tên của pha điều khiển tổng thể cấp giao lộ (VD: `NS_GREEN`). Cung cấp bối cảnh cho biết tại sao đèn này lại đang xanh hoặc đỏ.
* **`nextPhase`** *(Tùy chọn)*: String. Tên của pha điều khiển tiếp theo trong vòng lặp.
* **`greenDuration` / `yellowDuration` / `redDuration`** *(Tùy chọn)*: Integer (>= 0). Cấu hình thời gian mặc định (tính bằng giây) cho từng màu đèn trong điều kiện bình thường. Thuộc tính này cực kỳ quan trọng cho hệ thống **Recommendation Engine** sau này đánh giá xem thời lượng xanh hiện tại có lãng phí hay không.

### Chế độ Vận hành (Operational Mode)
* **`mode`** *(Tùy chọn)*: String. Trạng thái ra quyết định của hệ thống đèn.
  * `AUTO`: Simulator đang tự động chạy vòng lặp pha đèn (State Machine).
  * `MANUAL`: Cụm đèn đang bị cưỡng chế thời gian/màu sắc thông qua lệnh API từ Dashboard (Ví dụ: Ưu tiên mở đèn xanh khi phát hiện tai nạn).

## 3. Mối quan hệ (Relationships)
* **`refIntersection`** *(Bắt buộc)*: Relationship (URI). ID của giao lộ (`Intersection`) mà cụm đèn này được lắp đặt.
  * *Ràng buộc:* URI này phải trỏ đến một thực thể `Intersection` đang tồn tại trong Context Broker.
  * *Ví dụ JSON-LD:* 
    ```json
    "refIntersection": {
      "type": "Relationship",
      "object": "urn:ngsi-ld:Intersection:A"
    }
    ```