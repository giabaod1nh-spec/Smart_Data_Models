

---

# Hướng dẫn sử dụng nguồn mã cho dự án Smart Data Model

Mục đích của tài liệu này là giúp bạn hiểu và sử dụng nhanh các thành phần và mã nguồn liên quan đến Smart Data Model trong dự án Camera này.

- Dữ liệu mô hình được định nghĩa trong file model.yaml và được liên kết chặt chẽ với các tài liệu và payload mẫu trong thư mục dự án.
- Khi bạn cập nhật model.yaml, hãy chắc chắn cập nhật các payload/điển hình dữ liệu tương ứng trong thư mục ví dụ để bảo toàn tính nhất quán.

Tham khảo nhanh các file quan trọng:

- Định nghĩa dữ liệu mô hình: model.yaml
- Payload/ví dụ dữ liệu: examples
  - Ví dụ NGSIv2: example.json
  - Ví dụ NGSI-LD: example.jsonld
  - Các ví dụ khác có trong thư mục: example.json.csv, example.jsonld.csv
- Tài liệu mô tả và schema: spec.md, schema.json, schemaDTDL.json
- Mã nguồn mẫu sử dụng Data Model: code_for_using_dataModel.Device_Camera.py
- Ví dụ và hướng dẫn liên quan tới Pydantic/khai báo dữ liệu: code_for_using_pydantic.py
- Tài liệu tham khảo tổng quan (API/thiết kế): swagger.yaml

Mô tả các thành phần của Smart Data Model

- Schema (Lược đồ dữ liệu)
  - Biểu diễn kỹ thuật của Smart Data Model, định nghĩa cấu trúc và kiểu dữ liệu cho từng thuộc tính của thực thể (entity), các ràng buộc và các nhóm thuộc tính.
  - Mục tiêu: validation dữ liệu, đồng bộ cấu trúc giữa các hệ thống, giảm lỗi khi trao đổi dữ liệu.
- Specification (Đặc tả)
  - Tài liệu mô tả bằng ngôn ngữ tự nhiên dành cho con người, giải thích ý nghĩa của entity, thuộc tính, bắt buộc/tùy chọn, đơn vị đo và ví dụ sử dụng.
- URI (Uniform Resource Identifier)
  - Mỗi Entity và mỗi Attribute được gán một URI duy nhất, hỗ trợ liên kết dữ liệu và định danh mở rộng theo chuẩn Linked Data.
- Payload Examples (Ví dụ dữ liệu)
  - Cung cấp các ví dụ dữ liệu theo hai chuẩn NGSIv2 và NGSI-LD để áp dụng thực tế.
  - NGSIv2 dành cho Orion Context Broker truyền thống.
  - NGSI-LD hỗ trợ JSON-LD, ngữ nghĩa và liên kết dữ liệu.

Hướng dẫn sử dụng chi tiết

1. Định nghĩa Data Model

- Sử dụng file model.yaml để mô tả cấu trúc schema, các thuộc tính, và URIs liên quan.
- Lưu ý: mỗi khi bạn cập nhật model.yaml, nên đồng thời cập nhật các payload/ví dụ dữ liệu tương ứng để đảm bảo tính nhất quán giữa mô hình và dữ liệu thực thi.

2. Xem và tham khảo Specification

- Specification giúp người phát triển và thiết kế hệ thống hiểu đúng ý nghĩa và cách dùng của Data Model.
- Xem tại spec.md.

3. Kiểm tra và tham chiếu URI với các tài liệu liên quan

- Các URI và mô tả thực thể có thể tham chiếu trong [model.yaml] và liên kết với các tài liệu mô tả:
  - schema.json
  - schemaDTDL.json

4. Dữ liệu mẫu cho NGSIv2 và NGSI-LD

- NGSIv2 payload mẫu: example.json
- NGSI-LD payload mẫu: example.jsonld
- Các biến thể và CSV/JSON có sẵn trong thư mục examples

5. Sử dụng mã nguồn mẫu

- Mã mẫu để khởi tạo và làm việc với Data Model trong dự án:
  - code_for_using_dataModel.Device_Camera.py
  - code_for_using_pydantic.py
- Những ví dụ này cho thấy cách nạp Data Model, validate và sinh payload theo hai chuẩn NGSIv2 và NGSI-LD.

6. Tài liệu tham khảo và sơ đồ tổng quan

- Tổng quan và mô tả chi tiết có trong:
  - swagger.yaml
  - schema.json
  - schemaDTDL.json

Quy tắc thực hành

- Khi thay đổi model.yaml, hãy cập nhật các ví dụ dữ liệu tương ứng trong thư mục [examples/].
- Đảm bảo các URI được định nghĩa nhất quán và liên kết đúng với tài liệu mô tả để hỗ trợ Interoperability giữa các hệ thống.

Ghi chú bổ sung

- Đây là một hệ thống mẫu cho dự án Camera và có thể mở rộng cho các thiết bị/điểm đếm khác. Bạn có thể nhân rộng mẫu Data Model cho các thiết bị hoặc hệ thống khác bằng cách sao chép và chỉnh sửa model.yaml cùng với các payload tương ứng trong examples.

