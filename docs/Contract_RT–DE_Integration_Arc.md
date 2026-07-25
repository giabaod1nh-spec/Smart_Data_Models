# RT–DE Integration Architecture

## Status

**Implemented**

---

# 1. Background

Ban đầu hệ thống chỉ bao gồm phần Realtime (RT), chịu trách nhiệm mô phỏng giao thông và publish trạng thái hiện tại lên Orion Context Broker.

```
SUMO
    │
    ▼
Publisher
    │
    ▼
Orion Context Broker
```

Trong mô hình này, Data Engineering (DE) muốn xây dựng pipeline phải hiểu trực tiếp implementation của Publisher để biết Notification sẽ có cấu trúc như thế nào.

Điều này tạo ra một số vấn đề:

- Không có chuẩn giao tiếp chính thức giữa RT và DE.
- Thay đổi implementation của Publisher có thể làm hỏng pipeline của DE.
- Không có tiêu chuẩn để kiểm tra payload trước khi bắt đầu xây dựng pipeline.
- Không có ranh giới rõ ràng giữa hai nhóm.

---

# 2. Objectives

Kiến trúc được thay đổi với các mục tiêu sau:

- Chuẩn hóa giao diện trao đổi dữ liệu giữa RT và DE.
- Tách biệt implementation của hai bên.
- Cho phép RT và DE phát triển độc lập.
- Có cơ chế xác minh Notification trước khi bắt đầu Data Pipeline.
- Dễ dàng mở rộng và version hóa khi Contract thay đổi.

---

# 3. Architectural Changes

Sau khi bổ sung Contract, kiến trúc được thay đổi như sau:

```
                          contracts/
                              │
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                           │
        ▼                                           ▼
Realtime (RT)                              Data Engineering (DE)
        │                                           ▲
        ▼                                           │
SUMO → Publisher → Orion Context Broker → Notification
```

Contract trở thành **Source of Truth** cho dữ liệu trao đổi giữa hai phần của hệ thống.

Realtime và Data Engineering không còn phụ thuộc trực tiếp vào implementation của nhau mà chỉ cần tuân thủ cùng một Contract.

---

# 4. Repository Structure

```
Repository
│
├── Visualize/
│       Runtime implementation của Realtime
│
├── contracts/
│       Source of Truth của RT–DE Integration
│
├── integration/
│       Integration Verification
│
└── docs/
        Technical Documentation
```

---

# 5. Repository Guide

## Visualize/

**Vai trò**

Chứa toàn bộ runtime của Realtime.

Đây là nơi dữ liệu được sinh ra từ SUMO và publish lên Orion.

```
Visualize/
```

| Thành phần | Vai trò |
|------------|----------|
| app/traci_runner.py | Điều khiển SUMO và vòng lặp publish |
| integration/orion/entity_mapper.py | Mapping Simulation → NGSI-LD Entity |
| integration/orion/client.py | Gửi POST/PATCH tới Orion Context Broker |

Đây là implementation nội bộ của RT.

Data Engineering không phụ thuộc vào thư mục này.

---

## contracts/

**Vai trò**

Định nghĩa giao diện trao đổi dữ liệu giữa RT và DE.

Contract chỉ mô tả dữ liệu, không chứa implementation.

```
contracts/
```

| Thành phần | Vai trò |
|------------|----------|
| entity/ | Schema của các NGSI-LD Entity |
| simulation/ | Simulation metadata |
| topology/ | Network topology contract |
| delivery/ | Notification schema và Subscription template |
| tests/ | Offline contract validation |
| VERSION | Phiên bản Contract |
| README.md | Hướng dẫn sử dụng Contract |

Bất kỳ thay đổi nào ảnh hưởng tới Notification đều phải được cập nhật tại đây trước.

---

## integration/

**Vai trò**

Chứa toàn bộ công cụ dùng để kiểm chứng quá trình bàn giao giữa RT và DE.

Không phải runtime.

Không phải Data Pipeline.

```
integration/
```

| Thành phần | Vai trò |
|------------|----------|
| receiver/ | Temporary Notification Receiver dùng trong Integration Verification |
| scripts/register_subscription.py | Đăng ký Subscription lên Orion |
| test_orion_delivery_verification.py | Harness kiểm tra toàn bộ luồng RT → Orion → Notification |
| captured/ | Payload thực tế nhận được từ Orion |

Receiver chỉ phục vụ kiểm thử.

Receiver **không phải** Webhook của Data Engineering.

---

## docs/

Chứa tài liệu kỹ thuật của dự án.

Ví dụ:

- Architecture
- Implementation Plan
- Deployment Guide
- Verification Guide

Không chứa source code.

---

# 6. Responsibility Boundary

Sau khi bổ sung Contract, phạm vi trách nhiệm của hai nhóm được xác định rõ.

## Realtime

Realtime chịu trách nhiệm:

```
SUMO
    │
Snapshot Provider
    │
Publisher
    │
NGSI-LD Mapper
    │
Orion Context Broker
```

Realtime phải đảm bảo:

- Entity đúng Contract.
- Metadata đúng Contract.
- Publisher hoạt động đúng.
- Orion nhận đúng dữ liệu.
- Notification phát sinh đúng Contract.

Realtime **không chịu trách nhiệm**:

- Webhook
- ETL
- Bronze
- Silver
- Gold
- Analytics

---

## Data Engineering

Data Engineering bắt đầu từ Notification.

```
Notification
    │
Webhook
    │
Bronze
    │
Silver
    │
Gold
    │
Analytics
```

Data Engineering không cần biết:

- SUMO
- Publisher
- Entity Mapper
- Runtime của RT

Đầu vào duy nhất của DE là Notification đúng Contract.

---

# 7. Handoff Point

Điểm bàn giao giữa RT và DE được xác định tại Notification.

```
Realtime
    │
    ▼
Orion Notification
    │
    ▼
Data Engineering
```

Khi Orion gửi Notification thành công, trách nhiệm chuyển từ RT sang DE.

---

# 8. Runtime Flow

Luồng runtime của hệ thống:

```
SUMO
    │
    ▼
Publisher
    │
    ▼
Orion Context Broker
    │
    ▼
Subscription
    │
    ▼
Webhook
    │
    ▼
Bronze
    │
    ▼
Silver
    │
    ▼
Gold
```

Trong đó:

Publisher chỉ chịu trách nhiệm cập nhật Current Context lên Orion.

Orion chịu trách nhiệm:

- Lưu Current Context.
- Theo dõi thay đổi Entity.
- Kích hoạt Subscription.
- Gửi Notification.

Sau Notification, trách nhiệm thuộc về Data Engineering.

---

# 9. Integration Verification

Trước khi DE bắt đầu xây dựng pipeline, hệ thống thực hiện một milestone riêng để xác minh đường truyền dữ liệu.

```
SUMO
    │
Publisher
    │
Orion
    │
Subscription
    │
Temporary Receiver
```

Verification kiểm tra:

- SUMO publish nhiều chu kỳ.
- Orion cập nhật Entity.
- Subscription hoạt động.
- Notification được gửi.
- Payload đúng Contract.
- Payload được lưu làm evidence.

Receiver chỉ dùng cho quá trình kiểm thử.

Không phải Webhook production.

---

# 10. Development Workflow

Quy trình phát triển sau khi bổ sung Contract:

```
Realtime
    │
Publish Entity
    │
Orion
    │
Subscription
    │
Notification
    │
Integration Verification
    │
Data Engineering
```

Realtime có thể thay đổi implementation nội bộ miễn là Contract không thay đổi.

Data Engineering có thể phát triển pipeline độc lập miễn là Notification vẫn tuân thủ Contract.

---

# 11. Ownership Matrix

| Thành phần | Owner |
|------------|-------|
| SUMO Simulation | Realtime |
| Publisher | Realtime |
| Orion Context Broker | Realtime |
| contracts/ | Realtime + Data Engineering |
| integration/ | Realtime (Verification) |
| Webhook | Data Engineering |
| Bronze | Data Engineering |
| Silver | Data Engineering |
| Gold | Data Engineering |
| Analytics | Data Engineering |

---

# 12. Versioning Rules

Contract là giao diện chính thức giữa RT và DE.

Nếu cần thay đổi payload:

1. Cập nhật Contract.
2. Tăng VERSION.
3. RT cập nhật implementation.
4. DE cập nhật parser.
5. Thực hiện lại Integration Verification.

Không thay đổi Notification trực tiếp mà không cập nhật Contract.

---

# 13. Current State

Hiện tại repository đã hoàn thành các thành phần sau:

- Contract Layer.
- Notification Schema.
- Subscription Template.
- Integration Verification.
- Temporary Notification Receiver.
- Captured Notification từ môi trường thực.
- Integration Harness.

Realtime đã hoàn thành phần bàn giao dữ liệu.

Data Engineering có thể bắt đầu triển khai:

```
Notification
    │
Webhook
    │
Bronze
    │
Silver
    │
Gold
```

mà không cần phụ thuộc vào implementation của Publisher hoặc Runtime của Realtime, miễn là cả hai cùng tuân thủ Contract hiện tại.