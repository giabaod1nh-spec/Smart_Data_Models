$ErrorActionPreference='Stop'
$env:PYTHONPATH=(Resolve-Path .pytest_cache/phase4a-deps).Path
$p='.pytest_cache/perf-w2-base-01'
$compose=@('compose','-p','smart-perf-w2-base-01','-f',"$p/compose.json")
docker @compose up -d --no-deps --no-build --pull never --wait --wait-timeout 45 clickhouse
if ($LASTEXITCODE -ne 0) { throw 'Isolated ClickHouse startup failed' }
try {
    $probe=@($compose)+@('-f',"$p/probe-volumes.json")
    foreach ($mode in 'control-1','reuse-1','reuse-2','control-2') {
        $available=[long](python -c 'import psutil; print(psutil.virtual_memory().available)')
        if ($available -lt 1GB) { throw "Host memory below diagnostic floor: $available" }
        $files=@($probe)
        if ($mode.StartsWith('control')) { $files+=@('-f',"$p/control-bronze-code.json") }
        Get-Content "$p/probe-wal.py" -Raw | docker @files run --no-deps --rm -T --pull never --entrypoint python gold - --gold-path > "$p/bronze-reuse-$mode.json"
        if ($LASTEXITCODE -ne 0) { throw "Canonical probe failed: $mode" }
    }
    $reference=(Get-Content "$p/bronze-reuse-control-1.json" -Raw | ConvertFrom-Json).page_hashes
    if ($reference.Count -ne 32) { throw 'Control did not finish 32 members' }
    foreach ($mode in 'reuse-1','reuse-2','control-2') {
        $actual=(Get-Content "$p/bronze-reuse-$mode.json" -Raw | ConvertFrom-Json).page_hashes
        if (($reference -join ',') -ne ($actual -join ',')) { throw "Page fingerprint mismatch: $mode" }
    }
    $available=[long](python -c 'import psutil; print(psutil.virtual_memory().available)')
    if ($available -lt 1GB) { throw "Host memory below diagnostic floor: $available" }
    Get-Content "$p/probe-wal.py" -Raw | docker @probe -f "$p/proof-volume.json" run --no-deps --rm -T --pull never --entrypoint python gold - --replay-proof > "$p/retained-replay-proof.json"
    if ($LASTEXITCODE -ne 0) { throw 'Retained Gold proof did not complete' }
    $env:SILVER_LIVE_ACCEPTANCE='1'
    $env:GOLD_CORE_STAGE_B='1'
    $env:SILVER_CLICKHOUSE_HOST='127.0.0.1'
    $env:SILVER_CLICKHOUSE_PORT='18124'
    python -m pytest de/tests/silver/test_incremental_reader.py de/tests/gold_runtime/test_core_stage_b.py -q --basetemp=.pytest_cache/perf-bronze-reuse-real-temp --junitxml=.pytest_cache/perf-bronze-reuse-real.xml
    if ($LASTEXITCODE -ne 0) { throw 'Feed regression failed' }
} finally {
    docker @compose stop --timeout 20 clickhouse
}

