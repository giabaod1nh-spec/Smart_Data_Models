$ErrorActionPreference = 'Stop'
$profile = '.pytest_cache/perf-w2-base-01'
$compose = @('compose','-p','smart-perf-w2-base-01','-f',"$profile/compose.json")
try {
    docker @compose up -d --no-deps --no-build --pull never --wait --wait-timeout 45 clickhouse
    if ($LASTEXITCODE -ne 0) { throw 'Retained ClickHouse fixture failed startup' }
    Get-Content "$profile/audit-retained.py" -Raw |
        docker @compose -f "$profile/probe-volumes.json" -f "$profile/audit-volumes.json" run --no-deps --rm -T --pull never --entrypoint python gold - > "$profile/independent-durable-audit.json"
    if ($LASTEXITCODE -ne 0) { throw 'Retained audit did not pass' }
} finally {
    docker @compose stop --timeout 20 clickhouse
}
