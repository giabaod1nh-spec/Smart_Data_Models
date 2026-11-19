"""TopologyResolver — catalog link / route relevance."""
from __future__ import annotations

from typing import Any, Dict, Optional


class TopologyResolver:
    def __init__(self, catalog: Optional[Dict[str, Any]] = None):
        catalog = catalog or {}
        raw = catalog.get("inter_node_links") or {}
        if isinstance(raw, list):
            self.links = {l.get("id", str(i)): l for i, l in enumerate(raw)}
        else:
            self.links = dict(raw)

    def link_between(self, upstream: str, downstream: str) -> Optional[Dict[str, Any]]:
        for lid, link in self.links.items():
            if link.get("from_node") == upstream and link.get("to_node") == downstream:
                return {
                    "id": lid,
                    "edge": link.get("edge"),
                    "upstream_dir": link.get("from_approach"),
                    "downstream_dir": link.get("to_approach"),
                    **link,
                }
            up = link.get("upstream") or {}
            dn = link.get("downstream") or {}
            if up.get("intersection") == upstream and dn.get("intersection") == downstream:
                return {"id": lid, **link}
        return None
