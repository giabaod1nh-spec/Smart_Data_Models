# Dual-flow Realtime + Analytics E2E acceptance (Dashboard prep)
param(
    [string]$EvidenceRoot = "",
    [string]$BaseUrl = "http://localhost:8081",
    [string]$RunId = "",
    [string]$ScenarioId = "normal",
    [switch]$SkipSumo,
    [int]$MaxSimTime = 120,
    [int]$PipelineTimeoutSec = 600,
    [switch]$SkipServerRestart
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path $PSScriptRoot -Parent
$Stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
if (-not $EvidenceRoot) {
    $EvidenceRoot = Join-Path $RepoRoot "artifacts\server\dual_flow_acceptance\$Stamp"
}
New-Item -ItemType Directory -Force -Path $EvidenceRoot, "$EvidenceRoot\logs", "$EvidenceRoot\realtime\projector", "$EvidenceRoot\realtime\orion", "$EvidenceRoot\realtime\server_api", "$EvidenceRoot\realtime\comparisons", "$EvidenceRoot\analytics\server_api", "$EvidenceRoot\analytics\clickhouse", "$EvidenceRoot\auth", "$EvidenceRoot\sumo" | Out-Null

if (-not $RunId) {
    $RunId = "dual-flow-e2e-$Stamp"
}
@{ simulationRunId = $RunId; scenarioId = $ScenarioId; startedAt = (Get-Date).ToUniversalTime().ToString("o") } |
    ConvertTo-Json | Set-Content (Join-Path $EvidenceRoot "run_identity.json")

$Log = Join-Path $EvidenceRoot "logs\dual_flow_acceptance.log"
function Log($msg) { "$(Get-Date -Format o) $msg" | Tee-Object -FilePath $Log -Append }

$matrix = @()
function Add-Result($area, $case, $expected, $actual, $status, $evidence) {
    $script:matrix += [pscustomobject]@{ Area = $area; TestCase = $case; Expected = $expected; Actual = $actual; Status = $status; Evidence = $evidence }
}

function Invoke-ChQuery($sql) {
    $enc = [uri]::EscapeDataString($sql)
    return (curl.exe -s "http://localhost:8123/?database=smart_traffic&query=$enc").Trim()
}

function Wait-Pipeline($runId, $timeoutSec) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    $timeline = @()
    while ((Get-Date) -lt $deadline) {
        $raw = Invoke-ChQuery ('SELECT count() FROM kafka_raw_events WHERE simulation_run_id=''' + $runId + '''')
        $silver = Invoke-ChQuery ('SELECT count() FROM silver_fact_traffic_observation WHERE simulation_run_id=''' + $runId + '''')
        $gold60 = Invoke-ChQuery ('SELECT count() FROM gold_mart_intersection_window_summary WHERE namespace=''live'' AND simulation_run_id=''' + $runId + ''' AND window_size_sec=60')
        $ledger = Invoke-ChQuery 'SELECT count() FROM gold_processing_ledger WHERE namespace=''live'' AND disposition IN (''CHECKPOINTED'',''REPLAYED'',''QUARANTINED'')'
        $snap = @{ ts = (Get-Date).ToUniversalTime().ToString("o"); raw = $raw; silver = $silver; gold_mart_60 = $gold60; ledger = $ledger }
        $timeline += $snap
        Log "pipeline wait raw=$raw silver=$silver gold_mart=$gold60 ledger=$ledger"
        if ([int]$raw -ge 20 -and [int]$silver -ge 20 -and [int]$gold60 -ge 1 -and [int]$ledger -ge 1) {
            return @{ ok = $true; final = $snap; timeline = $timeline }
        }
        Start-Sleep -Seconds 5
    }
    return @{ ok = $false; final = $timeline[-1]; timeline = $timeline }
}

function Restart-SpringServer {
    Log "Restarting Spring Server on 8081 (clear analytics readiness cache)..."
    $conn = Get-NetTCPConnection -LocalPort 8081 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($conn) {
        Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 3
    }
    $serverDir = Join-Path $RepoRoot "server"
    $jdk = "C:\Program Files\Java\jdk-22"
    if (-not (Test-Path $jdk)) { $jdk = $env:JAVA_HOME }
    $bootLog = Join-Path $EvidenceRoot "logs\spring_boot.log"
    $env:JAVA_HOME = $jdk
    $env:SPRING_PROFILES_ACTIVE = "local,analytics"
    $env:SERVER_PORT = "8081"
    $proc = Start-Process -FilePath (Join-Path $serverDir "mvnw.cmd") `
        -ArgumentList @("spring-boot:run", "-Dspring-boot.run.jvmArguments=-Dserver.port=8081") `
        -WorkingDirectory $serverDir -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $bootLog -RedirectStandardError $bootLog
    $deadline = (Get-Date).AddMinutes(4)
    while ((Get-Date) -lt $deadline) {
        try {
            $h = Invoke-RestMethod -Uri "$BaseUrl/api/system/health" -TimeoutSec 5
            if ($h.server -eq "UP") {
                Log "Spring Server UP pid=$($proc.Id)"
                return @{ ok = $true; pid = $proc.Id; log = $bootLog }
            }
        } catch { }
        Start-Sleep -Seconds 5
    }
    throw "Spring Server failed to start within 4 minutes"
}

function Invoke-ApiGet($uri, $session, $outPath, $expectStatus) {
    try {
        $r = Invoke-WebRequest -Uri $uri -WebSession $session -UseBasicParsing
        $r.Content | Set-Content $outPath
        return @{ code = [int]$r.StatusCode; body = $r.Content; ok = ($expectStatus -contains [int]$r.StatusCode) }
    } catch {
        if ($_.Exception.Response) {
            $code = [int]$_.Exception.Response.StatusCode
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $body = $reader.ReadToEnd()
            $body | Set-Content $outPath
            return @{ code = $code; body = $body; ok = ($expectStatus -contains $code) }
        }
        throw
    }
}

Log "=== DUAL-FLOW ACCEPTANCE START runId=$RunId evidence=$EvidenceRoot ==="

# --- SUMO producer ---
if (-not $SkipSumo) {
    Log "Running SUMO traci_runner max_sim=$MaxSimTime..."
    $env:ORION_PUBLISH_ENABLED = "false"
    $env:ORION_SYNC_PUBLISH = "false"
    $env:KAFKA_OUTBOX_ENABLED = "true"
    $env:KAFKA_PUBLISH_ENABLED = "false"
    $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
    $sumoLog = Join-Path $EvidenceRoot "sumo\sumo_run.log"
    Push-Location (Join-Path $RepoRoot "Visualize")
    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $sumoProc = Start-Process -FilePath "python" -ArgumentList @(
            "-m", "app.traci_runner", "--no-gui", "--fast", "--nodes", "A,B,C,D",
            "--max-sim-time", "$MaxSimTime", "--simulation-run-id", $RunId
        ) -WorkingDirectory (Get-Location) -Wait -PassThru -NoNewWindow `
            -RedirectStandardOutput $sumoLog -RedirectStandardError "$sumoLog.err"
        if (Test-Path "$sumoLog.err") {
            Get-Content "$sumoLog.err" -ErrorAction SilentlyContinue | Add-Content $sumoLog
            Remove-Item "$sumoLog.err" -ErrorAction SilentlyContinue
        }
        if ($sumoProc.ExitCode -ne 0) {
            Add-Result "Producer" "sumo" "exit 0" "exit $($sumoProc.ExitCode)" "FAIL" "sumo/sumo_run.log"
            throw "SUMO failed exit $($sumoProc.ExitCode)"
        }
        Add-Result "Producer" "sumo" "exit 0" "exit 0" "PASS" "sumo/sumo_run.log"
        Log "SUMO PASS wall exit=$($sumoProc.ExitCode)"
    } finally {
        $ErrorActionPreference = $prevEap
        Pop-Location
    }
} else {
    Log "SkipSumo - using existing pipeline data"
    Add-Result "Producer" "sumo" "skipped" "skipped" "SKIP" "sumo/sumo_run.log"
}

# --- Wait DE pipeline + projector ---
$pipe = Wait-Pipeline $RunId $PipelineTimeoutSec
$pipe | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $EvidenceRoot "analytics\clickhouse\pipeline_wait.json")
if ($pipe.ok) { Add-Result "Pipeline" "gold_ready" "ledger+mart>0" "ok" "PASS" "analytics/clickhouse/pipeline_wait.json" }
else { Add-Result "Pipeline" "gold_ready" "ledger+mart>0" "timeout" "FAIL" "analytics/clickhouse/pipeline_wait.json" }

$projDeadline = (Get-Date).AddSeconds(120)
$projOk = $false
while ((Get-Date) -lt $projDeadline) {
    try {
        $proj = Invoke-RestMethod -Uri "http://localhost:8093/current-run" -TimeoutSec 5
        if ($proj.simulationRunId -eq $RunId) { $projOk = $true; break }
        Log "projector current-run=$($proj.simulationRunId) waiting for $RunId"
    } catch { Log "projector current-run error: $($_.Exception.Message)" }
    Start-Sleep -Seconds 3
}
$proj | ConvertTo-Json -Depth 8 | Set-Content (Join-Path $EvidenceRoot "realtime\projector\current_run.json")
if ($projOk) { Add-Result "Realtime" "projector_run_match" $RunId $proj.simulationRunId "PASS" "realtime/projector/current_run.json" }
else {
    $projRun = if ($proj -and $proj.simulationRunId) { $proj.simulationRunId } else { "none" }
    Add-Result "Realtime" "projector_run_match" $RunId $projRun "PARTIAL" "realtime/projector/current_run.json"
}

# --- Restart Spring (analytics readiness cache) ---
if (-not $SkipServerRestart) {
    Restart-SpringServer | Out-Null
    Add-Result "Server" "restart" "UP" "UP" "PASS" "logs/spring_boot.log"
} else {
    Add-Result "Server" "restart" "skipped" "skipped" "SKIP" "logs/spring_boot.log"
}

# --- Auth ---
$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
try {
    Invoke-WebRequest -Method Get -Uri "$BaseUrl/api/realtime/intersections/A" -UseBasicParsing | Out-Null
    Add-Result "Auth" "anonymous_realtime" "401/403" "unexpected 200" "FAIL" "auth/anonymous_realtime.txt"
    exit 1
} catch {
    $code = [int]$_.Exception.Response.StatusCode
    "$code" | Set-Content (Join-Path $EvidenceRoot "auth\anonymous_realtime.txt")
    if ($code -in 401, 403) { Add-Result "Auth" "anonymous_realtime" "401/403" $code "PASS" "auth/anonymous_realtime.txt" }
    else { Add-Result "Auth" "anonymous_realtime" "401/403" $code "FAIL" "auth/anonymous_realtime.txt"; exit 1 }
}

$loginBody = @{ username = "admin"; password = "admin123" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/auth/login" -ContentType "application/json" -Body $loginBody -WebSession $session | Out-Null
Add-Result "Auth" "admin_login" "200" "200" "PASS" "auth/login.json"
Log "ADMIN login PASS"

# --- Orion upstream ---
$orion = Invoke-RestMethod -Uri "http://localhost:1026/ngsi-ld/v1/entities/urn:ngsi-ld:Intersection:A" -Headers @{ Accept = "application/ld+json" }
$orion | ConvertTo-Json -Depth 10 | Set-Content (Join-Path $EvidenceRoot "realtime\orion\intersection_A.json")

# --- Realtime API ---
$rtPath = Join-Path $EvidenceRoot "realtime\server_api\intersection_A.json"
$rtResult = Invoke-ApiGet "$BaseUrl/api/realtime/intersections/A" $session $rtPath @(200, 503)
$rt = $null
if ($rtResult.code -eq 200) {
    $rt = $rtResult.body | ConvertFrom-Json
    $rtRun = $rt.data.metadata.simulationRunId
    $runMatch = ($rtRun -eq $RunId -or $rtRun -eq $proj.simulationRunId)
    Add-Result "Realtime" "intersection_A_status" "200" "200" "PASS" $rtPath
    Add-Result "Realtime" "intersection_A_run_id" $RunId $rtRun $(if ($runMatch) { "PASS" } else { "PARTIAL" }) $rtPath
    Log "Realtime A -> 200 runId=$rtRun"
} else {
    Add-Result "Realtime" "intersection_A_status" "200" "503" "PARTIAL" $rtPath
    Log "Realtime A -> 503 (stale/upstream)"
}

# --- Analytics endpoints (Dashboard contract) ---
$common = @{ simulationRunId = $RunId; scenarioId = $ScenarioId; windowSizeSec = 60 }
$analyticsEndpoints = @(
    @{ Path = "/api/analytics/intersections/A/windows"; Name = "intersection_windows"; Expect = @(200) },
    @{ Path = "/api/analytics/intersections/A/directions/windows"; Name = "direction_windows"; Expect = @(200) },
    @{ Path = "/api/analytics/intersections/A/comparisons"; Name = "comparisons"; Expect = @(200) },
    @{ Path = "/api/analytics/intersections/A/trends"; Name = "trends"; Expect = @(200) },
    @{ Path = "/api/analytics/congestion"; Name = "congestion"; Expect = @(200) },
    @{ Path = "/api/analytics/priority"; Name = "priority"; Expect = @(200) },
    @{ Path = "/api/analytics/signals/operation-windows"; Name = "signal_operation"; Expect = @(200) },
    @{ Path = "/api/analytics/network/windows"; Name = "network"; Expect = @(503) }
)

$analyticsPass = $true
foreach ($ep in $analyticsEndpoints) {
    $qs = ($common.GetEnumerator() | ForEach-Object { "{0}={1}" -f $_.Key, [uri]::EscapeDataString([string]$_.Value) }) -join '&'
    $uri = "$BaseUrl$($ep.Path)?$qs"
    $out = Join-Path $EvidenceRoot "analytics\server_api\$($ep.Name).json"
    $res = Invoke-ApiGet $uri $session $out $ep.Expect
    $status = if ($res.ok) { "PASS" } elseif ($res.code -eq 503 -and $ep.Name -ne "network") { "BLOCKED"; $analyticsPass = $false } else { "FAIL"; $analyticsPass = $false }
    Add-Result "Analytics" $ep.Name ($ep.Expect -join "/") $res.code $status $out
    Log "Analytics $($ep.Name) -> $($res.code) [$status]"
}

# --- ClickHouse authoritative probe ---
$chCount = Invoke-ChQuery ('SELECT count() FROM gold_mart_intersection_window_summary WHERE simulation_run_id=''' + $RunId + ''' AND window_size_sec=60')
$chCount | Set-Content (Join-Path $EvidenceRoot "analytics\clickhouse\intersection_count.txt")
Log "ClickHouse intersection rows for run: $chCount"
if ([int]$chCount -gt 0) { Add-Result "Analytics" "clickhouse_live_rows" ">0" $chCount "PASS" "analytics/clickhouse/intersection_count.txt" }
else { Add-Result "Analytics" "clickhouse_live_rows" ">0" $chCount "FAIL" "analytics/clickhouse/intersection_count.txt"; $analyticsPass = $false }

# --- Realtime vs authoritative comparison ---
$cmp = @{
    simulationRunId = $RunId
    scenarioId = $ScenarioId
    projectorRunId = $proj.simulationRunId
    realtimeRunId = if ($rt) { $rt.data.metadata.simulationRunId } else { $null }
    clickhouseGoldRows = [int]$chCount
    checkedAt = (Get-Date).ToUniversalTime().ToString("o")
}
if ($rt -and $rt.data.intersection) {
    $cmp.intersectionId = $rt.data.intersection.id
}
$cmp | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $EvidenceRoot "realtime\comparisons\realtime_comparison.json")
$isoRealtime = ($rtResult.code -eq 200)
$isoAnalytics = $analyticsPass -and $pipe.ok
Add-Result "Isolation" "ISO_dual_flow" "both 200" "rt=$isoRealtime an=$isoAnalytics" $(if ($isoRealtime -and $isoAnalytics) { "PASS" } else { "PARTIAL" }) "realtime/comparisons/realtime_comparison.json"

# --- Verdict ---
$verdict = if ($isoRealtime -and $isoAnalytics) { "DUAL-FLOW BACKEND E2E PASS - NETWORK OVERVIEW DEFERRED" }
elseif ($isoAnalytics -and -not $isoRealtime) { "ANALYTICS PASS - REALTIME PARTIAL" }
elseif ($isoRealtime -and -not $isoAnalytics) { "REALTIME PASS - ANALYTICS PARTIAL" }
else { "DUAL-FLOW PARTIAL - SEE acceptance_matrix.csv" }

@{ verdict = $verdict; simulationRunId = $RunId; evidenceRoot = $EvidenceRoot; finishedAt = (Get-Date).ToUniversalTime().ToString("o") } |
    ConvertTo-Json | Set-Content (Join-Path $EvidenceRoot "verdict.json")

$matrix | Export-Csv (Join-Path $EvidenceRoot "acceptance_matrix.csv") -NoTypeInformation
Log "VERDICT: $verdict"
Log "=== DUAL-FLOW ACCEPTANCE COMPLETE ==="
Write-Host $verdict
if ($isoRealtime -and $isoAnalytics) { exit 0 } else { exit 2 }
