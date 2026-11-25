param(
    [Parameter(Mandatory)]
    [ValidatePattern('^perf-w2-[a-z0-9-]+$')]
    [string]$ProfileName,
    [ValidateRange(61, 599)]
    [int]$SimulationSeconds = 330,
    [ValidateRange(61, 600)]
    [int]$SourceWallSeconds = 430,
    [ValidateRange(1, 599)]
    [int]$DrainSeconds = 130,
    [ValidateSet(60, 300)]
    [int[]]$RequireWindow = @(60, 300)
)

$ErrorActionPreference = 'Stop'
if ($SimulationSeconds -gt $SourceWallSeconds -or $SourceWallSeconds + $DrainSeconds -gt 600) {
    throw 'The paced diagnostic requires enough source time and a combined source/drain ceiling of 600 seconds'
}
$env:PYTHONPATH = (Resolve-Path .pytest_cache/phase4a-deps).Path
$profile = ".pytest_cache/$ProfileName"
$project = "smart-$ProfileName"
$py = (Get-Command python).Source
$docker = (Get-Command docker).Source
# Use a prepared disposable project with its own volumes and host ports.
$compose = @('compose','-p',$project,'-f',"$profile/compose.json")
if (Test-Path -LiteralPath "$profile/profile-overlay.json") {
    $compose += @('-f',"$profile/profile-overlay.json")
}
if ((Test-Path -LiteralPath "$profile/samples.jsonl") -or
    (Test-Path -LiteralPath "$profile/producer/kafka_outbox/v1-outbox.sqlite3")) {
    throw 'Prepare a fresh fixture for a paced diagnostic'
}

function Test-Measurement {
    $r = Get-Content "$profile/samples.jsonl" -Tail 1 -ErrorAction SilentlyContinue |
        ConvertFrom-Json -ErrorAction SilentlyContinue
    if ($r.measurement.stop_reason) { throw "Measurements invalid: $($r.measurement.stop_reason)" }
    if ($sampler.HasExited) { throw "Measurement sampler stopped (exit $($sampler.ExitCode))" }
    $sampleFile = Get-Item -LiteralPath "$profile/samples.jsonl" -ErrorAction SilentlyContinue
    if (-not $sampleFile -or ([DateTime]::UtcNow - $sampleFile.LastWriteTimeUtc).TotalSeconds -gt 15) {
        throw 'No current measurement sample'
    }
    if ($r -and (-not $r.host.available_memory_bytes -or $r.host.available_memory_bytes -lt 1GB)) {
        throw 'Host memory below 1 GiB or unavailable'
    }
}

$started = $false
try {
    $null = Resolve-Path -LiteralPath "$profile/compose.json"
    $started = $true
    docker @compose up -d --no-build --pull never --wait --wait-timeout 90
    if ($LASTEXITCODE -ne 0) { throw 'Isolated startup failed' }
    $available = [long](& $py -c 'import psutil; print(psutil.virtual_memory().available)')
    Write-Output "Host memory available after startup: $available bytes"
    if ($available -lt 2500000000) { throw "Insufficient startup headroom: $available bytes" }
    $sampleSeconds = $SourceWallSeconds + $DrainSeconds + 20
    $sampler = Start-Process -FilePath $py -ArgumentList @('-m','de.tools.gold_core_capacity','sample',$profile,'--seconds',$sampleSeconds) -WindowStyle Hidden -PassThru -RedirectStandardError "$profile/sampler.stderr.log" -RedirectStandardOutput "$profile/sampler.stdout.log"
    Start-Sleep -Seconds 5
    Test-Measurement
    $producer = Start-Process -FilePath $py -ArgumentList @('-m','de.tools.gold_core_capacity','produce',$profile,'--sumo-step','0.01','--seconds',$SimulationSeconds,'--realtime','--run-id',$ProfileName) -WindowStyle Hidden -PassThru -RedirectStandardError "$profile/producer.stderr.log" -RedirectStandardOutput "$profile/producer.stdout.log"
    $clock = [Diagnostics.Stopwatch]::StartNew()
    while (-not $producer.HasExited) {
        Start-Sleep -Seconds 3
        Test-Measurement
        if ($clock.Elapsed.TotalSeconds -gt $SourceWallSeconds) { throw 'Producer wall budget exhausted' }
    }
    if ($producer.ExitCode -ne 0) { throw "Producer failed: $($producer.ExitCode)" }
    Test-Measurement
    Write-Output "Production elapsed: $($clock.Elapsed.TotalSeconds) seconds"
    $clock.Restart()
    $nextOracle = 0
    $windows = @($RequireWindow | ForEach-Object { '--require-window'; "$_" })
    $status = 2
    while ($clock.Elapsed.TotalSeconds -lt $DrainSeconds) {
        Test-Measurement
        if ($oracle -and $oracle.HasExited) {
            $status = $oracle.ExitCode
            $oracle = $null
            Move-Item -LiteralPath "$profile/bounded-oracle.pending.json" -Destination "$profile/bounded-oracle.json" -Force
            if ($status -eq 0) { Write-Output "Required work drained in $($clock.Elapsed.TotalSeconds) seconds"; break }
            if ($status -ne 2) { throw "Oracle failed unexpectedly: $status" }
            $nextOracle = $clock.Elapsed.TotalSeconds + 15
        }
        if (-not $oracle -and $clock.Elapsed.TotalSeconds -ge $nextOracle) {
            $oracleArgs = $compose + @('exec','-T','gold','python','-m','de.tools.gold_core_capacity','oracle','/app/Visualize/artifacts') + $windows
            $oracle = Start-Process -FilePath $docker -ArgumentList $oracleArgs -WindowStyle Hidden -PassThru -RedirectStandardOutput "$profile/bounded-oracle.pending.json" -RedirectStandardError "$profile/oracle.stderr.log"
        }
        Start-Sleep -Seconds 3
    }
    if ($status -ne 0) { 'Drain budget reached with incomplete evidence' | Set-Content "$profile/incomplete-drain.txt" }
} catch {
    $_.Exception.Message | Set-Content "$profile/early-stop.txt"
    throw
} finally {
    if ($oracle -and -not $oracle.HasExited) { Stop-Process -InputObject $oracle }
    if ($producer -and -not $producer.HasExited) { Stop-Process -InputObject $producer }
    if ($sampler -and -not $sampler.HasExited) { Stop-Process -InputObject $sampler }
    if ($started) { docker @compose stop --timeout 20 }
}
exit $status
