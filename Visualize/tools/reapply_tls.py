"""Update selected <tlLogic> blocks with canonical OSM programs.

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

def extract_tllogic_blocks(programs_xml: str) -> list[str]:
    blocks = re.findall(r"<tlLogic\b[^>]*>.*?</tlLogic>", programs_xml, flags=re.DOTALL)
    if not blocks:
        raise SystemExit("No <tlLogic> blocks found in programs file")
    return [b.strip() for b in blocks]


def reapply(net_text: str, blocks: list[str]) -> str:
    updated = net_text
    for block in blocks:
        id_match = re.search(r'<tlLogic\b[^>]*\bid="([^"]+)"', block)
        if not id_match:
            raise SystemExit("A canonical tlLogic block is missing its id")
        tls_id = id_match.group(1)
        pattern = re.compile(
            rf'[ \t]*<tlLogic\b(?=[^>]*\bid="{re.escape(tls_id)}")[^>]*>.*?</tlLogic>\s*',
            re.DOTALL,
        )
        updated, count = pattern.subn("    " + block + "\n", updated, count=1)
        if count != 1:
            raise SystemExit(f"Expected one existing tlLogic for {tls_id}, found {count}")
    return updated


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
