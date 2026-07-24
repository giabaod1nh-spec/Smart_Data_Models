"""Replace <tlLogic> blocks in intersection.net.xml with canonical programs.

Usage (from Visualize/):
  python tools/reapply_tls.py
  python tools/reapply_tls.py --programs Visualize/tls/programs_phase38.xml
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_NET = ROOT / "Visualize" / "intersection.net.xml"
DEFAULT_PROGRAMS = ROOT / "Visualize" / "tls" / "programs_phase38.xml"

_TLLOGIC_BLOCK = re.compile(
    r"[ \t]*<tlLogic\b[^>]*>.*?</tlLogic>\s*",
    re.DOTALL,
)


def extract_tllogic_blocks(programs_xml: str) -> list[str]:
    blocks = re.findall(r"<tlLogic\b[^>]*>.*?</tlLogic>", programs_xml, flags=re.DOTALL)
    if not blocks:
        raise SystemExit("No <tlLogic> blocks found in programs file")
    return [b.strip() for b in blocks]


def reapply(net_text: str, blocks: list[str]) -> str:
    cleaned, n = _TLLOGIC_BLOCK.subn("", net_text)
    if n == 0:
        raise SystemExit("No existing <tlLogic> blocks found in net file")
    # Insert after last non-internal <edge> ... before first <junction>
    insert = "\n    " + "\n    ".join(blocks) + "\n\n"
    m = re.search(r"\n[ \t]*<junction\b", cleaned)
    if not m:
        raise SystemExit("Could not find <junction> insertion point in net file")
    return cleaned[: m.start()] + "\n" + insert + cleaned[m.start() + 1 :]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--net", type=Path, default=DEFAULT_NET)
    ap.add_argument("--programs", type=Path, default=DEFAULT_PROGRAMS)
    args = ap.parse_args()
    blocks = extract_tllogic_blocks(args.programs.read_text(encoding="utf-8"))
    new_text = reapply(args.net.read_text(encoding="utf-8"), blocks)
    args.net.write_text(new_text, encoding="utf-8")
    print(f"Re-applied {len(blocks)} tlLogic blocks -> {args.net}")


if __name__ == "__main__":
    main()
