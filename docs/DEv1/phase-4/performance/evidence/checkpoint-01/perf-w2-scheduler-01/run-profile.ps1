$ErrorActionPreference = 'Stop'
$env:PYTHONPATH = (Resolve-Path .pytest_cache/phase4a-deps).Path
$profile = '.pytest_cache/perf-w2-scheduler-01'
$project = 'smart-perf-w2-scheduler-01'
$py = (Get-Command python).Source
$compose = @('compose','-p',$project,'-f',"$profile/compose.json")
if (Test-Path -LiteralPath "$profile/producer/kafka_outbox/v1-outbox.sqlite3") {
    throw 'A matched run requires a fresh source'
}
docker @compose up -d --no-build --pull never --wait --wait-timeout 90
if ($LASTEXITCODE -ne 0) { throw 'Isolated services failed startup' }
try {
    $available = [long](& $py -c 'import psutil; print(psutil.virtual_memory().available)')
    Write-Output "Host memory available after startup: $available bytes"
    if ($available -lt 1GB) { throw 'Host memory below diagnostic floor' }
    $sampler = Start-Process -FilePath $py -ArgumentList @('-m','de.tools.gold_core_capacity','sample',$profile,'--seconds','245') -WindowStyle Hidden -PassThru -RedirectStandardError "$profile/sampler.stderr.log" -RedirectStandardOutput "$profile/sampler.stdout.log"
    Start-Sleep -Seconds 5
    $producer = Start-Process -FilePath $py -ArgumentList @('-m','de.tools.gold_core_capacity','produce',$profile,'--sumo-step','0.01','--seconds','150','--realtime','--run-id','perf-w2-scheduler-01') -WindowStyle Hidden -PassThru -RedirectStandardError "$profile/producer.stderr.log" -RedirectStandardOutput "$profile/producer.stdout.log"
    $deadline = [DateTime]::UtcNow.AddSeconds(195)
    while (-not $producer.HasExited) {
        Start-Sleep -Seconds 3
        $last = Get-Content "$profile/samples.jsonl" -Tail 1 | ConvertFrom-Json -ErrorAction SilentlyContinue
        $reason = if ([DateTime]::UtcNow -gt $deadline) { 'producer wall budget exhausted' }
            elseif ($last.host.available_memory_bytes -and $last.host.available_memory_bytes -lt 1GB) { 'host memory below 1 GiB' }
            elseif ($last.services.silver.state -eq 'FAULTED' -or $last.services.gold.state -eq 'FAULTED') { 'integrity or permanent dependency fault' }
        if ($reason -and -not $producer.HasExited) {
            Stop-Process -InputObject $producer
            $reason | Set-Content "$profile/early-stop.txt"
            throw $reason
        }
    }
    if ($producer.ExitCode -ne 0) {
        Get-Content "$profile/producer.stderr.log" -Tail 10
        throw 'Producer failed'
    }
    Start-Sleep -Seconds 45
    docker @compose exec -T gold python -m de.tools.gold_core_capacity oracle /app/Visualize/artifacts > "$profile/bounded-oracle.json"
    Write-Output "Oracle status $LASTEXITCODE"
} finally {
    if ($sampler -and -not $sampler.HasExited) {
        Stop-Process -InputObject $sampler
    }
    docker @compose stop --timeout 20
}

