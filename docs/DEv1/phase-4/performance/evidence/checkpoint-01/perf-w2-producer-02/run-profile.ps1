$ErrorActionPreference = 'Stop'
$env:PYTHONPATH = (Resolve-Path .pytest_cache/phase4a-deps).Path
$profile = '.pytest_cache/perf-w2-producer-02'
$project = 'smart-perf-w2-producer-02'
$py = (Get-Command python).Source
$compose = @('compose','-p',$project,'-f',"$profile/compose.json")
if (Test-Path -LiteralPath "$profile/producer/kafka_outbox/v1-outbox.sqlite3") {
    throw 'Pacing comparison requires a fresh source'
}
docker @compose up -d --no-deps --no-build --pull never --wait --wait-timeout 60 kafka
if ($LASTEXITCODE -ne 0) { throw 'Isolated Kafka startup failed' }
try {
    docker @compose run --no-deps --rm -T --pull never topic
    if ($LASTEXITCODE -ne 0) { throw 'Topic setup failed' }
    $available = [long](& $py -c 'import psutil; print(psutil.virtual_memory().available)')
    if ($available -lt 1GB) { throw 'Host memory below diagnostic floor' }
    $sampler = Start-Process -FilePath $py -ArgumentList @('-m','de.tools.gold_core_capacity','sample',$profile,'--seconds','132','--source-only') -WindowStyle Hidden -PassThru -RedirectStandardError "$profile/sampler.stderr.log" -RedirectStandardOutput "$profile/sampler.stdout.log"
    Start-Sleep -Seconds 5
    $producer = Start-Process -FilePath $py -ArgumentList @('-m','de.tools.gold_core_capacity','produce',$profile,'--sumo-step','0.01','--seconds','90','--realtime','--run-id','perf-w2-producer-02') -WindowStyle Hidden -PassThru -RedirectStandardError "$profile/producer.stderr.log" -RedirectStandardOutput "$profile/producer.stdout.log"
    $deadline = [DateTime]::UtcNow.AddSeconds(120)
    while (-not $producer.HasExited) {
        Start-Sleep -Seconds 3
        $last = Get-Content "$profile/samples.jsonl" -Tail 1 | ConvertFrom-Json -ErrorAction SilentlyContinue
        $reason = if ([DateTime]::UtcNow -gt $deadline) { 'producer wall budget exhausted' }
            elseif ($last.host.available_memory_bytes -and $last.host.available_memory_bytes -lt 1GB) { 'host memory below 1 GiB' }
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
    Start-Sleep -Seconds 5
} finally {
    if ($sampler -and -not $sampler.HasExited) {
        Stop-Process -InputObject $sampler
    }
    docker @compose stop --timeout 20 kafka
}

