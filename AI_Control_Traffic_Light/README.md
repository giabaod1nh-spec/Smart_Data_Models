# Cooperative Multi-Agent DQN — điều khiển đèn tín hiệu

Hệ thống 4 DQN agent (A–D) với Communication Bus in-memory, tích hợp SUMO/TraCI qua `Visualize/simulation/backend.py`.

## Cài đặt

```bash
pip install -r AI_Control_Traffic_Light/requirements.txt
# hoặc từ Visualize/
pip install -r Visualize/requirements.txt
```

## Train

```bash
# Từ repo root
python -m AI_Control_Traffic_Light.training.train_multi_agent \
  --mode cooperative \
  --episodes 100 \
  --scenario heavy_traffic \
  --seed 42
```

Chế độ so sánh: `fixed`, `independent`, `cooperative`.

## Evaluate

```bash
python -m AI_Control_Traffic_Light.training.evaluate \
  --scenario spillback \
  --episodes 5 \
  --modes fixed independent cooperative
```

## Runtime (Live simulation)

1. Khởi động SUMO + Control API như bình thường (`traci_runner.py`)
2. Bật chế độ ADAPTIVE qua API hoặc UI:

```bash
curl -X POST http://localhost:9090/control-mode -H "Content-Type: application/json" \
  -d '{"mode":"ADAPTIVE"}'
```

3. Xem trạng thái agent:

```bash
curl http://localhost:9090/rl/status
```

Model được load từ `AI_Control_Traffic_Light/models/agent_{A,B,C,D}.keras`.

## Kiến trúc

- **Agent A–D** ↔ giao lộ J1–J4 (SUMO TLS)
- **Neighbors:** A↔B, A↔C, B↔D, C↔D
- **Actions:** KEEP, SWITCH, EXTEND +5s, EXTEND +10s
- **Reward:** local + global + spillback penalty
