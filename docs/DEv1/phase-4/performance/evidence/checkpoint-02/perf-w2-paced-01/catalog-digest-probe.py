"""Cost of recomputing an unchanged archive catalog digest."""
from dataclasses import asdict
import json
from pathlib import Path
import sys
import time

PROFILE = Path(__file__).resolve().parent
sys.path.insert(0, str(PROFILE.parent.parent))

from de.silver.archive_resolver import ArchiveResolver


def main():
    resolver = ArchiveResolver(PROFILE / "producer")
    name = PROFILE.name
    manifest = json.loads((resolver.root / "runs" / name / "run_manifest.json").read_bytes())
    coords = (name, manifest["topologyDigest"], manifest["runGeneration"])
    expected = asdict(resolver.resolve(*coords))
    results = []
    for mode in ("control", "candidate", "candidate", "control", "control", "candidate"):
        resolver._last_catalog_digest = None
        started = time.perf_counter()
        for iteration in range(128):
            if mode == "control":
                resolver._last_catalog_digest = None
            observed = resolver.resolve(*coords)
            if observed.catalog_file_hash != expected["catalog_file_hash"]:
                raise RuntimeError("archive changed during the paired probe")
            if time.perf_counter() - started > 5:
                raise RuntimeError("archive-only probe exceeded the five-second pass limit")
        elapsed = time.perf_counter() - started
        if asdict(observed) != expected:
            raise RuntimeError("run context differed")
        results.append(dict(mode=mode, iterations=128, wall_seconds=elapsed))
    print(json.dumps(results, sort_keys=True))


if __name__ == "__main__":
    main()
