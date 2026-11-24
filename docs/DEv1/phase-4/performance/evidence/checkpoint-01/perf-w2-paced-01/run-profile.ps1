$ErrorActionPreference = 'Stop'
$env:PYTHONPATH = (Resolve-Path .pytest_cache/phase4a-deps).Path
$profile = '.pytest_cache/perf-w2-paced-01'
$project = 'smart-perf-w2-paced-01'
$py = (Get-Command python).Source
$compose = @('compose','-p',$project,'-f',"$profile/compose.json",'-f',"$profile/profile-overlay.json")
if (Test-Path -LiteralPath "$profile/producer/kafka_outbox/v1-outbox.sqlite3") {
    throw 'The paced diagnostic requires a fresh source'
}

function Test-Headroom {
    $r = Get-Content "$profile/samples.jsonl" -Tail 1 -ErrorAction SilentlyContinue |
        ConvertFrom-Json -ErrorAction SilentlyContinue
    if ($r.host.available_memory_bytes -and $r.host.available_memory_bytes -lt 1GB) {
        throw 'host memory below 1 GiB'
    }
    if ($r.services.silver.state -eq 'FAULTED' -or $r.services.gold.state -eq 'FAULTED') {
        throw 'integrity or permanent dependency fault'
    }
}

try {
    docker @compose up -d --no-build --pull never --wait --wait-timeout 90
    if ($LASTEXITCODE -ne 0) { throw 'Isolated startup failed' }
    $available = [long](& $py -c 'import psutil; print(psutil.virtual_memory().available)')
    Write-Output "Host memory available after startup: $available bytes"
    if ($available -lt 2500000000) { throw "Insufficient startup headroom: $available bytes" }
    $sampler = Start-Process -FilePath $py -ArgumentList @('-m','de.tools.gold_core_capacity','sample',$profile,'--seconds','570') -WindowStyle Hidden -PassThru -RedirectStandardError "$profile/sampler.stderr.log" -RedirectStandardOutput "$profile/sampler.stdout.log"
    Start-Sleep -Seconds 5
    $producer = Start-Process -FilePath $py -ArgumentList @('-m','de.tools.gold_core_capacity','produce',$profile,'--sumo-step','0.01','--seconds','330','--realtime','--run-id','perf-w2-paced-01') -WindowStyle Hidden -PassThru -RedirectStandardError "$profile/producer.stderr.log" -RedirectStandardOutput "$profile/producer.stdout.log"
    $clock = [Diagnostics.Stopwatch]::StartNew()
    while (-not $producer.HasExited) {
        Start-Sleep -Seconds 3
        if ($clock.Elapsed.TotalSeconds -gt 430) { throw 'producer wall budget exhausted' }
        Test-Headroom
    }
    if ($producer.ExitCode -ne 0) { throw "Producer failed: $($producer.ExitCode)" }
    Write-Output "Production elapsed: $($clock.Elapsed.TotalSeconds) seconds"
    $clock.Restart()
    $nextOracle = 0
    while ($clock.Elapsed.TotalSeconds -lt 130) {
        Test-Headroom
        if ($clock.Elapsed.TotalSeconds -ge $nextOracle) {
            docker @compose exec -T gold python -m de.tools.gold_core_capacity oracle /app/Visualize/artifacts --require-window 60 --require-window 300 > "$profile/bounded-oracle.json"
            $status = $LASTEXITCODE
            if ($status -eq 0) { Write-Output "Both durations drained at $($clock.Elapsed.TotalSeconds) seconds"; break }
            if ($status -ne 2) { throw "Oracle failed unexpectedly: $status" }
            $nextOracle += 15
        }
        Start-Sleep -Seconds 3
    }
    if ($status -ne 0) { Write-Output "Drain budget reached with incomplete evidence" }
} catch {
    $_.Exception.Message | Set-Content "$profile/early-stop.txt"
    throw
} finally {
    if ($producer -and -not $producer.HasExited) { Stop-Process -InputObject $producer }
    if ($sampler -and -not $sampler.HasExited) { Stop-Process -InputObject $sampler }
    docker @compose stop --timeout 20
}
