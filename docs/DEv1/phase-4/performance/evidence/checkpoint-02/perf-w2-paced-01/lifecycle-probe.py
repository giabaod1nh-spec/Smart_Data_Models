"""Time Silver registration with the real retained archive and new scratch SQLite."""
from collections import defaultdict
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time

from de.silver.archive_resolver import ArchiveResolver
from de.silver.lifecycle_refresher import LifecycleRefresher


archive = Path("/app/Visualize/artifacts")
run_id = "perf-w2-paced-01"
manifest = json.loads((archive / "runs" / run_id / "run_manifest.json").read_text())
digest, generation = manifest["topologyDigest"], manifest["runGeneration"]
elapsed = defaultdict(list)
identities = []
with TemporaryDirectory(prefix="lifecycle-probe-") as tmp:
    for index, mode in enumerate(("double_resolve", "reuse", "reuse", "double_resolve")):
        resolver = ArchiveResolver(archive)
        refresher = LifecycleRefresher(Path(tmp) / f"{index}.sqlite3", resolver)
        started = time.monotonic()
        for _ in range(64):
            context = resolver.resolve(run_id, digest)
            if mode == "reuse":
                refresher.register_verified(context)
            else:
                refresher.register(run_id, digest, generation)
            if time.monotonic() - started > 20:
                raise RuntimeError("Lifecycle microbenchmark exceeded its budget")
        elapsed[mode].append(time.monotonic()-started)
        with refresher._connect() as db:
            identities.append(tuple(db.execute("SELECT run_id,generation,topology_digest,manifest_identity "
                                               "FROM silver_known_runs_v1").fetchone()))
assert all(identity == identities[0] for identity in identities)
print(json.dumps(dict(samples_per_pass=64, elapsed_seconds=elapsed, durable_identity_matches=True)))
