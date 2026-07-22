# SUMO TraCI Backend (Visualize) — v1

Một chiều: **SUMO → TraCI → Snapshot → entity_generator → Orion**.

Simulator 1D trong `simulator/` **không bị thay thế/xóa**; chạy độc lập như cũ.

## Yêu cầu

1. [Eclipse SUMO](https://eclipse.dev/sumo/) (đã thử với 1.27.x)
2. Python 3.10+
3. Orion-LD (tùy chọn — có thể chạy `--no-orion`)

## Cài đặt (Windows PowerShell)

```powershell
# 1) SUMO
$env:SUMO_HOME = "D:\SUMO"
$env:PATH = "$env:SUMO_HOME\bin;$env:PATH"

# 2) Python deps
cd D:\Giao_trinh_dai_hoc\K2_N4\TTTN\Smart_Traffic\Smart_Data_Models\Visualize
pip install -r requirements.txt

# 3) (tuỳ chọn) Orion
$env:ORION_URL = "http://localhost:1026"
```

Sao chép `.env.example` nếu muốn tham khảo biến môi trường.

## Chạy

```powershell
# GUI realtime — xem mo phong, chi tat khi ban dong cua so / Ctrl+C
python traci_runner.py --gui --no-orion

# GUI + publish Orion (node A = TLS J1)
python traci_runner.py --gui

# Headless smoke (chay nhanh, tu dung sau 30s sim)
python traci_runner.py --no-gui --no-orion --max-sim-time 30

# GUI chay nhanh het toc do (kho xem)
python traci_runner.py --gui --no-orion --fast
```

Simulator 1D cũ:

```powershell
cd ..\simulator
python main.py
```

## Mapping v1

| NGSI | SUMO |
|------|------|
| Intersection `A` | TLS / junction `J1` |
| North | edge `J3J1` (lanes `J3J1_0`, `J3J1_1`) |
| East | `J2J1` |
| South | `S1J1` |
| West | `W1J1` |

Phase: `0=NS_GREEN`, `1=NS_YELLOW`, `2=EW_GREEN`, `3=EW_YELLOW`.

URN ví dụ: `urn:ngsi-ld:Intersection:A`, `urn:ngsi-ld:VehicleSensor:A:NORTHBOUND`.

## Kiểm tra Orion

```powershell
curl "$env:ORION_URL/ngsi-ld/v1/entities/urn:ngsi-ld:Intersection:A"
curl "$env:ORION_URL/ngsi-ld/v1/entities?type=VehicleSensor"
```

## Test (không cần SUMO binary)

```powershell
cd Visualize
pytest tests/test_v1.py -v
```

## Cấu trúc

| File | Vai trò |
|------|---------|
| `config.py` | Mapping, PCU, env |
| `traci_runner.py` | Entry: step loop + publish |
| `sumo_backend.py` | Facade start/step/snapshot/control |
| `sumo_snapshot_provider.py` | TraCI → snapshot contract |
| `sumo_signal_controller.py` | TLS phase map + force/green |
| `sumo_scenario_manager.py` | normal/peak/rain/accident |
| `Visualize/intersection.*` | SUMO network / routes |

## TODO / hạn chế v1

- Chỉ **publish J1→A** (mạng J1–J4 vẫn chạy trong GUI).
- Chưa có E1/E2 detectors — queue/arrival dùng lane + vehicle API.
- `queue_by_movement` ước lượng theo route turn (net 2 làn, không 3 lane movement).
- `intersection_box_blocked`, `vehicles_blocked_at_entry`, `yellow_commitment_count`, `preemption_active` = default (TODO trong snapshot).
- `count_exited_network` chưa tích lũy toàn bộ arrived.
- Occupancy lấy từ SUMO lane occupancy (khác công thức lateral moto của simulator 1D).
- Unknown `vType` → `PCU_FALLBACK=1.0` + warning log.

## PCU (`config.PCU_FACTORS`)

| vType | PCU | Ghi chú |
|-------|-----|---------|
| motorcycle | 0.24 | = `simulator/models.py` |
| car | 1.00 | = models.py |
| bus / truck | 2.50 | = models.py |
| container | 2.50 | SUMO-only |
| ambulance / police | 1.00 | SUMO-only |
| firetruck | 2.50 | SUMO-only |
