$ErrorActionPreference = 'Stop'
$fixture = '.pytest_cache/perf-w2-paced-01'
$evidence = "$fixture/recovery-approved"
$project = 'smart-perf-w2-paced-01'
$compose = @('compose', '-p', $project, '-f', "$fixture/compose.json", '-f',
    "$fixture/profile-overlay.json", '-f', "$evidence/volumes.json")
$start = (Get-Content -LiteralPath "$fixture/downstream-drain-approved.samples.jsonl" -TotalCount 1 |
    ConvertFrom-Json).utc
$limit = ([DateTimeOffset] $start).AddSeconds(430)
$available = [long](python -c 'import psutil;print(psutil.virtual_memory().available)')
if ($available -lt 1GB) { throw 'Host memory below the runtime floor' }
$running = @(docker ps --filter "label=com.docker.compose.project=$project" -q)
if ($LASTEXITCODE -ne 0 -or $running.Count -ne 0) { throw 'Incident workers must be stopped' }
Get-Content -LiteralPath "$evidence/snapshot.py" -Raw |
    docker @compose run --no-deps --rm -T --pull never --entrypoint python gold - |
    Set-Content -LiteralPath "$evidence/state.json"
if ($LASTEXITCODE -ne 0) { throw 'Cannot seal the stopped SQLite databases' }
$available = [long](python -c 'import psutil;print(psutil.virtual_memory().available)')
if ($available -lt 2500000000 -or ($limit - [DateTimeOffset]::UtcNow).TotalSeconds -lt 35) {
    'Physical audit deferred by headroom or remaining time' | Set-Content "$evidence/physical-deferred.txt"
    exit 0
}
$clickhouse = "$project-clickhouse-1"
$audit = $null
$code = 0
try {
    docker start $clickhouse | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not start retained ClickHouse for read-only audit' }
    $readyDeadline = [DateTimeOffset]::UtcNow.AddSeconds(12)
    do {
        $available = [long](python -c 'import psutil;print(psutil.virtual_memory().available)')
        if ($available -lt 1GB) { throw 'Host memory below the runtime floor' }
        $health = docker inspect --format '{{.State.Health.Status}}' $clickhouse
        if ($health -eq 'healthy') { break }
        Start-Sleep -Seconds 1
    } while ([DateTimeOffset]::UtcNow -lt $readyDeadline)
    if ($health -ne 'healthy' -or $available -lt 2500000000) {
        throw "Physical audit readiness gate failed with ClickHouse health '$health' and $available available bytes"
    }
    $env:PYTHONPATH = (Resolve-Path .pytest_cache/perf-resume-deps).Path
    $auditArgs = @{
        FilePath = (Get-Command python).Source
        ArgumentList = "$evidence/audit-physical.py"
        PassThru = $true
        WindowStyle = 'Hidden'
        RedirectStandardOutput = "$evidence/physical-audit.json"
        RedirectStandardError = "$evidence/physical-audit.stderr.log"
    }
    $audit = Start-Process @auditArgs
    while (-not $audit.HasExited) {
        $available = [long](python -c 'import psutil;print(psutil.virtual_memory().available)')
        if ($available -lt 1GB -or [DateTimeOffset]::UtcNow -ge $limit.AddSeconds(-12)) {
            throw 'Physical audit stopped at the resource or wall guard'
        }
        Start-Sleep -Seconds 1
    }
    $code = $audit.ExitCode
} finally {
    if ($audit -and -not $audit.HasExited) { Stop-Process -InputObject $audit }
    docker stop --timeout 8 $clickhouse | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not stop the retained ClickHouse audit service' }
}
exit $code
