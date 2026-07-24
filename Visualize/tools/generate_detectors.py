"""Generate detectors.add.xml from intersection.net.xml lane lengths."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from observation.detector_manager import build_detectors_xml

NET = ROOT / "Visualize" / "intersection.net.xml"
OUT = ROOT / "Visualize" / "detectors.add.xml"


def main() -> None:
    text = NET.read_text(encoding="utf-8")
    lengths = {
        m.group(1): float(m.group(2))
        for m in re.finditer(r'<lane id="([^"]+)"[^>]*length="([0-9.]+)"', text)
    }
    xml = build_detectors_xml(lengths)
    OUT.write_text(xml, encoding="utf-8")
    print(f"Wrote {OUT} lanes={len(lengths)} E1={xml.count('inductionLoop')} E2={xml.count('laneAreaDetector')}")


if __name__ == "__main__":
    main()
