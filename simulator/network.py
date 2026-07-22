"""
network.py — Mạng lưới đường: Intersection Node + Boundary Node + Edge

4 giao lộ thật (A, B, C, D) có đèn tín hiệu, có NGSI-LD entity.
5 Boundary Node (N1, W1, S1, E1, E2) chỉ dùng để spawn/despawn xe của
toàn mạng lưới — KHÔNG có đèn, KHÔNG publish NGSI-LD.

Sơ đồ:

                        N1
                         |
                         | edge_N1_A / edge_A_N1 (200m)
                         v
      W1 == edge_W1_A/edge_A_W1(250m) == A == edge_A_B/edge_B_A(300m) == B == edge_B_E1/edge_E1_B(220m) == E1
                         |                                                |
                edge_A_C/edge_C_A (280m)                          edge_B_D/edge_D_B (260m)
                         v                                                v
                         C == edge_C_D/edge_D_C (300m) ==================== D
                         |                                                |
                edge_C_S1/edge_S1_C (200m)                        edge_D_E2/edge_E2_D (210m)
                         v                                                v
                        S1                                              E2
"""
import random
import networkx as nx

INTERSECTION_NODES = ["A", "B", "C", "D"]
BOUNDARY_NODES = ["N1", "W1", "S1", "E1", "E2"]
ALL_NODES = INTERSECTION_NODES + BOUNDARY_NODES

# Mỗi cạnh có: from, to, length_m, from_dir (hướng xe RỜI intersection nguồn,
# None nếu nguồn là boundary), to_dir (hướng xe VÀO intersection đích,
# None nếu đích là boundary)
EDGES = {
    "edge_N1_A": {"from": "N1", "to": "A", "length_m": 200, "from_dir": None,   "to_dir": "North"},
    "edge_A_N1": {"from": "A",  "to": "N1", "length_m": 200, "from_dir": "North", "to_dir": None},

    "edge_W1_A": {"from": "W1", "to": "A", "length_m": 250, "from_dir": None,   "to_dir": "West"},
    "edge_A_W1": {"from": "A",  "to": "W1", "length_m": 250, "from_dir": "West",  "to_dir": None},

    "edge_A_B": {"from": "A", "to": "B", "length_m": 300, "from_dir": "East", "to_dir": "West"},
    "edge_B_A": {"from": "B", "to": "A", "length_m": 300, "from_dir": "West", "to_dir": "East"},

    "edge_A_C": {"from": "A", "to": "C", "length_m": 280, "from_dir": "South", "to_dir": "North"},
    "edge_C_A": {"from": "C", "to": "A", "length_m": 280, "from_dir": "North", "to_dir": "South"},

    "edge_B_E1": {"from": "B",  "to": "E1", "length_m": 220, "from_dir": "East", "to_dir": None},
    "edge_E1_B": {"from": "E1", "to": "B",  "length_m": 220, "from_dir": None,   "to_dir": "East"},

    "edge_B_D": {"from": "B", "to": "D", "length_m": 260, "from_dir": "South", "to_dir": "North"},
    "edge_D_B": {"from": "D", "to": "B", "length_m": 260, "from_dir": "North", "to_dir": "South"},

    "edge_C_D": {"from": "C", "to": "D", "length_m": 300, "from_dir": "East", "to_dir": "West"},
    "edge_D_C": {"from": "D", "to": "C", "length_m": 300, "from_dir": "West", "to_dir": "East"},

    # ── Cong phu (redundant gateway) — them de giam ty le fallback cua
    # compute_route_avoiding (da do thuc te: 62% fallback khi chi co 1
    # cong duy nhat cho moi boundary node). KHONG the them canh cheo
    # A<->D truc tiep vi A da dung het ca 4 huong (North=N1, South=C,
    # East=B, West=W1) — vi pham gia dinh "giao lo 4 huong". Thay vao do,
    # dung dung cac huong CON TRONG de tao cong thu 2 cho 2 boundary hay
    # bi fallback nhat:
    #   B con trong huong North  -> them cong phu cho S1 (truoc chi qua C)
    #   D con trong huong South  -> them cong phu cho E1 (truoc chi qua B)
    # A khong co huong trong nen N1/W1 van la single-gateway — gioi han
    # nay duoc ghi nhan ro rang, khong co cach khac neu giu dung mo hinh
    # giao lo 4 huong chuan.
    "edge_B_S1": {"from": "B",  "to": "S1", "length_m": 340, "from_dir": "North", "to_dir": None},
    "edge_S1_B": {"from": "S1", "to": "B",  "length_m": 340, "from_dir": None,    "to_dir": "North"},

    "edge_D_E1": {"from": "D",  "to": "E1", "length_m": 300, "from_dir": "South", "to_dir": None},
    "edge_E1_D": {"from": "E1", "to": "D",  "length_m": 300, "from_dir": None,    "to_dir": "South"},

    "edge_C_S1": {"from": "C",  "to": "S1", "length_m": 200, "from_dir": "South", "to_dir": None},
    "edge_S1_C": {"from": "S1", "to": "C",  "length_m": 200, "from_dir": None,    "to_dir": "South"},

    "edge_D_E2": {"from": "D",  "to": "E2", "length_m": 210, "from_dir": "East", "to_dir": None},
    "edge_E2_D": {"from": "E2", "to": "D",  "length_m": 210, "from_dir": None,   "to_dir": "East"},
}

_graph = nx.DiGraph()
for edge_id, e in EDGES.items():
    _graph.add_edge(e["from"], e["to"], weight=e["length_m"], edge_id=edge_id)

_EDGE_LOOKUP = {}
for edge_id, e in EDGES.items():
    _EDGE_LOOKUP[(e["from"], e["to"])] = edge_id


def pick_spawn_boundary() -> str:
    """Xe CHỈ được sinh tại Boundary Node — không bao giờ sinh tại A/B/C/D."""
    return random.choice(BOUNDARY_NODES)


def pick_destination(source: str, scenario: str = None) -> str:
    """
    Chọn điểm đến.
    85% boundary khác (có OD bias theo scenario nếu có trong OD_MATRIX),
    15% intersection nội đô.
    """
    from scenarios import OD_MATRIX  # import cục bộ tránh vòng phụ thuộc lúc load

    if random.random() < 0.15:
        return random.choice(INTERSECTION_NODES)

    candidates = [n for n in BOUNDARY_NODES if n != source]
    if not candidates:
        return source

    if scenario and scenario in OD_MATRIX:
        od = OD_MATRIX[scenario]
        weights = [max(od.get((source, dest), 0.05), 0.01) for dest in candidates]
        total = sum(weights)
        weights = [w / total for w in weights]
        return random.choices(candidates, weights=weights, k=1)[0]

    return random.choice(candidates)


def compute_route(source: str, target: str) -> list:
    """Route ngắn nhất theo trọng số chiều dài (mét)."""
    if source == target:
        return [source]
    try:
        return nx.shortest_path(_graph, source=source, target=target, weight="weight")
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return [source]


def compute_route_avoiding(source: str, target: str, avoid_node: str) -> list:
    """Route ngắn nhất nhưng loại bỏ 1 node khỏi đồ thị (dùng cho re-routing)."""
    if source == target:
        return [source]
    graph_copy = _graph.copy()
    if avoid_node in graph_copy.nodes and avoid_node not in (source, target):
        graph_copy.remove_node(avoid_node)
    try:
        return nx.shortest_path(graph_copy, source=source, target=target, weight="weight")
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return compute_route(source, target)  # fallback: route gốc (có thể qua avoid_node)


def get_edge_between(node_a: str, node_b: str) -> str:
    edge_id = _EDGE_LOOKUP.get((node_a, node_b))
    if edge_id is None:
        raise ValueError(f"No edge between {node_a} and {node_b}")
    return edge_id


def get_reachable_nodes(source: str) -> list:
    return [n for n in _graph.nodes if n != source and nx.has_path(_graph, source, n)]
