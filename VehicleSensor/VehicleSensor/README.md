
---

# Hướng dẫn sử dụng nguồn mã — VehicleSensor Smart Data Model

Mục đích của tài liệu này là giúp bạn hiểu và sử dụng nhanh các thành phần liên quan đến Smart Data Model **VehicleSensor**.

- Dữ liệu mô hình được định nghĩa trong `vehiclesensor_model.yaml` và liên kết với `doc/spec.md` cùng payload mẫu trong `examples/`.
- Khi cập nhật `vehiclesensor_model.yaml`, hãy cập nhật đồng thời các payload trong `examples/` để giữ nhất quán.

## File quan trọng

- Định nghĩa mô hình: `vehiclesensor_model.yaml`
- Đặc tả: `doc/spec.md`
- Schema: `schema.json`, `schemaDTDL.json`, `schema.sql`
- API: `swagger.yaml`
- Ví dụ:
  - Key-values: `examples/example.json`, `examples/example.jsonld`
  - Normalized NGSI-LD: `examples/example-normalized.json`, `examples/example-normalized.jsonld`
- Code mẫu: `code/code_for_using_pydantic.py`

## Vai trò entity

**VehicleSensor** mô tả metrics giao thông theo **một hướng tiếp cận** tại nút giao — PCU, queue-by-movement, arrival rate (input CBR+GA). Có thể liên kết tới Intersection, Camera và TrafficLight qua các relationship tương ứng.

## URN pattern

```
urn:ngsi-ld:VehicleSensor:{intersectionId}:{direction}
# ví dụ: urn:ngsi-ld:VehicleSensor:Intersection001:NORTHBOUND
```

## Quy tắc

- Đổi model → cập nhật examples + spec.
- Không publish từng Vehicle lên Orion — chỉ tổng hợp qua VehicleSensor.
