# Analytics Business Service — runtime acceptance (BS-11)
param(
    [string]$BaseUrl = "http://localhost:8080",
    [string]$AdminUser = "admin",
    [string]$AdminPass = "admin123",
    [string]$EvidenceDir = ""
)

$ErrorActionPreference = "Stop"
if (-not $EvidenceDir) {
    $EvidenceDir = Join-Path $PSScriptRoot "..\..\artifacts\server\business_service\20260805T170500Z\runtime_acceptance"
}
New-Item -ItemType Directory -Force -Path $EvidenceDir | Out-Null

$log = Join-Path $EvidenceDir "acceptance.log"
function Log($msg) { "$(Get-Date -Format o) $msg" | Tee-Object -FilePath $log -Append }

$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
$loginBody = @{ username = $AdminUser; password = $AdminPass } | ConvertTo-Json
try {
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/auth/login" -ContentType "application/json" -Body $loginBody -WebSession $session | Out-Null
    Log "PASS login"
} catch {
    Log "FAIL login $($_.Exception.Message)"; exit 1
}

function Invoke-AnalyticsGet($path, [hashtable]$Query, [int[]]$ExpectStatus = @(200)) {
    $qs = ($Query.GetEnumerator() | ForEach-Object { "{0}={1}" -f [uri]::EscapeDataString($_.Key), [uri]::EscapeDataString([string]$_.Value) }) -join "&"
    $uri = if ($qs) { "$BaseUrl$path`?$qs" } else { "$BaseUrl$path" }
    try {
        $resp = Invoke-WebRequest -Method Get -Uri $uri -WebSession $session -UseBasicParsing
        $code = [int]$resp.StatusCode
        $body = $resp.Content
    } catch {
        if ($_.Exception.Response) {
            $code = [int]$_.Exception.Response.StatusCode
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            $body = $reader.ReadToEnd()
        } else { throw }
    }
    $ok = $ExpectStatus -contains $code
    Log ("{0} GET {1} -> {2}" -f $(if ($ok) { "PASS" } else { "FAIL" }), $path, $code)
    if ($body) { Log $body.Substring(0, [Math]::Min(500, $body.Length)) }
    if (-not $ok) { throw "Unexpected status $code for $path" }
    return @{ Status = $code; Body = $body }
}

$common = @{
    simulationRunId = "run-1"
    scenarioId      = "normal"
    windowSizeSec   = 60
}

Log "--- intersection window 60s ---"
Invoke-AnalyticsGet "/api/analytics/intersections/int-001/windows" $common

Log "--- network scaffold 503 ---"
Invoke-AnalyticsGet "/api/analytics/network/windows" $common -ExpectStatus @(503)

Log "--- invalid window size 400 ---"
Invoke-AnalyticsGet "/api/analytics/intersections/int-001/windows" (@{ simulationRunId = "run-1"; scenarioId = "normal"; windowSizeSec = 120 }) -ExpectStatus @(400)

Log "--- valid empty range 200 ---"
Invoke-AnalyticsGet "/api/analytics/intersections/int-001/windows" (@{
    simulationRunId = "run-1"; scenarioId = "normal"; windowSizeSec = 60
    fromSimulationSec = 1000; toSimulationSec = 2000
})

Log "--- realtime still up ---"
Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/system/health" | Out-Null
Log "PASS public health"

Log "ACCEPTANCE COMPLETE"
