"""Freeze a replay manifest from current read-only Bronze/archive authorities."""
import argparse
import json
from pathlib import Path

from de.silver.archive_resolver import ArchiveResolver
from de.silver.config import SilverSettings, validate_namespace_id
from de.silver.replay import validate_manifest
from de.silver.source_binding import verify_bronze_source, SourceBoundaryChangedError


def freeze(run_id: str, replay_id: str) -> Path:
    validate_namespace_id(replay_id)
    if not run_id or run_id in {'.', '..'} or any(c in run_id for c in '/\\\0'):
        raise ValueError('invalid archived run id')
    settings = SilverSettings()
    resolver = ArchiveResolver(Path(settings.v1_archive_root))
    # Resolve through the historical package; never through active-run selection.
    manifest, _ = resolver._load(resolver.root / 'runs' / run_id / 'run_manifest.json')
    context = resolver.resolve(run_id, manifest['topologyDigest'])
    source = verify_bronze_source(Path(settings.v1_bronze_path), settings.v1_source_namespace)
    doc = dict(manifest_version='silver-replay-v1', replay_run_id=replay_id,
        source_namespace=settings.v1_source_namespace, source_binding=source,
        change_high_water=source['change_high_water'],
        invalidation_high_water=source['invalidation_high_water'],
        simulation_run_id=run_id, run_generation=context.generation,
        topology_digest=context.topology_digest, manifest_identity=context.manifest_identity,
        manifest_file_hash=context.manifest_file_hash, catalog_file_hash=context.catalog_file_hash,
        reader_version='1', processor_version='1', schema_version='008')
    validate_manifest(doc, replay_id)
    if verify_bronze_source(Path(settings.v1_bronze_path), settings.v1_source_namespace) != source:
        raise SourceBoundaryChangedError()
    target = Path(settings.checkpoint_path).parent / 'acceptance' / (replay_id + '.manifest.json')
    target.parent.mkdir(parents=True, exist_ok=True)
    # Existing IDs/manifests are never silently overwritten.
    with target.open('x', encoding='utf-8') as stream:
        json.dump(doc, stream, indent=2)
    return target


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--replay-id', required=True)
    args = parser.parse_args()
    print(freeze(args.run_id, args.replay_id))
