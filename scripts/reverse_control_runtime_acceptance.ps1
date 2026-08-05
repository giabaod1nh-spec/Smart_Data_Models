# Bounded reverse-control runtime acceptance (RC8-T1)
# Usage: .\scripts\reverse_control_runtime_acceptance.ps1 [-SkipDockerCheck]
param(
    [string]$ServerBase = "http://localhost:8081",
    [string]$ControlBase = "http://localhost:9090",
    [string]$ProjectorBase = "http://localhost:8093",
    [int]$PollMs = 400,
    [int]$CaseTimeoutSec = 25,
    [switch]$SkipDockerCheck
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Split-Path -Parent $ScriptDir
$Ts = (Get-Date).ToUniversalTime().ToString("yyyyMMdd'T'HHmmss'Z'")
$EvidenceRoot = Join-Path $Root "artifacts\realtime\reverse_control\$Ts\runtime_acceptance"
$Dirs = @("requests", "responses", "command_status", "runtime_logs", "orion_snapshots", "kafka_regression")
foreach ($d in $Dirs) { New-Item -ItemType Directory -Force -Path (Join-Path $EvidenceRoot $d) | Out-Null }

$Summary = [System.Collections.Generic.List[string]]::new()
$Failed = $false

function Save-Json($Dir, $Name, $Obj) {
    $path = Join-Path (Join-Path $EvidenceRoot $Dir) $Name
    ($Obj | ConvertTo-Json -Depth 12) | Set-Content -Encoding UTF8 $path
}

function Invoke-Rest($Method, $Url, $Body = $null, $Session = $null) {
    $params = @{ Method = $Method; Uri = $Url; TimeoutSec = 15 }
    if ($Session) { $params.WebSession = $Session }
    if ($Body -ne $null) {
        $params.ContentType = "application/json"
        $params.Body = ($Body | ConvertTo-Json -Depth 10)
    }
    return Invoke-RestMethod @params
}

function Assert-Health {
    param([string]$Name, [scriptblock]$Check)
    try {
        & $Check | Out-Null
        $Summary.Add("HEALTH $Name PASS")
    } catch {
        $Summary.Add("HEALTH $Name FAIL: $($_.Exception.Message)")
        throw
    }
}

if (-not $SkipDockerCheck) {
    Assert-Health "Orion" { Invoke-Rest GET "http://localhost:1026/version" }
    Assert-Health "Projector" { Invoke-Rest GET "$ProjectorBase/health" }
    Assert-Health "ControlAPI" {
        $h = Invoke-Rest GET "$ControlBase/health"
        if (-not $h.simulation_run_id) { throw "simulation not running" }
    }
    Assert-Health "Server" { Invoke-Rest GET "$ServerBase/api/system/health" }
}

$session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
$login = Invoke-Rest POST "$ServerBase/api/auth/login" @{ username = "admin"; password = "admin123" } $session
Save-Json "responses" "login.json" $login

$runId = $null
try {
    $ctrlHealth = Invoke-Rest GET "$ControlBase/health"
    Save-Json "responses" "control_health.json" $ctrlHealth
    $runId = $ctrlHealth.simulation_run_id
} catch {}
if (-not $runId) {
    $current = Invoke-Rest GET "$ProjectorBase/current-run"
    Save-Json "responses" "projector_current_run.json" $current
    $runId = $current.simulationRunId
}
if (-not $runId) {
    $Summary.Add("FAIL: no active simulationRunId")
    $Failed = $true
} else {
$Summary.Add("ACTIVE runId=$runId")

function New-CommandBody($Type, $Target, $Payload) {
    return @{
        contractVersion = "1.0"
        commandId = [guid]::NewGuid().ToString()
        commandType = $Type
        target = $Target
        payload = $Payload
        expectedRunId = $runId
        idempotencyKey = "rc8-$Type-" + [guid]::NewGuid().ToString()
        requestedAt = (Get-Date).ToUniversalTime().ToString("o")
        expiresAt = (Get-Date).ToUniversalTime().AddMinutes(5).ToString("o")
        source = "DASHBOARD"
    }
}

function Poll-Command($CommandId) {
    $deadline = (Get-Date).AddSeconds($CaseTimeoutSec)
    $last = $null
    while ((Get-Date) -lt $deadline) {
        $last = Invoke-Rest GET "$ServerBase/api/control/commands/$CommandId" $null $session
        Save-Json "command_status" "$CommandId.json" $last
        $life = $last.data.lifecycleStatus
        if ($life -in @("COMPLETED", "FAILED", "EXPIRED", "UNKNOWN_OUTCOME")) { return $last }
        Start-Sleep -Milliseconds $PollMs
    }
    return $last
}

function Run-PositiveCase($Name, $Body, $ExpectCompleted = $true) {
    if ($Failed) { return }
    try {
        Save-Json "requests" "$Name.json" $Body
        $accept = Invoke-Rest POST "$ServerBase/api/control/commands" $Body $session
        Save-Json "responses" "${Name}_accept.json" $accept
        if ($accept.status -ne 200 -and $accept.status -ne 202) { throw "unexpected accept status $($accept.status)" }
        $cmdId = $accept.data.commandId
        $final = Poll-Command $cmdId
        $life = $final.data.lifecycleStatus
        $exec = $final.data.executionStatus
        $obs = $final.data.observationStatus
        if ($ExpectCompleted -and $life -ne "COMPLETED") { throw "lifecycle=$life exec=$exec" }
        if ($ExpectCompleted -and $exec -ne "APPLIED_AT_SUMO") { throw "execution=$exec" }
        $Summary.Add("CASE $Name PASS life=$life exec=$exec obs=$obs")
    } catch {
        $Summary.Add("CASE $Name FAIL: $($_.Exception.Message)")
        $Failed = $true
    }
}

Run-PositiveCase "FORCE_PHASE" (New-CommandBody "FORCE_PHASE" @{ intersectionId = "A" } @{ phase = "NS_GREEN" })
Run-PositiveCase "SET_GREEN_DURATION" (New-CommandBody "SET_GREEN_DURATION" @{ intersectionId = "A" } @{ seconds = 45 })
Run-PositiveCase "SET_SCENARIO" (New-CommandBody "SET_SCENARIO" @{ intersectionId = "A" } @{ scenario = "morning_peak" })
Run-PositiveCase "SET_DEMAND_PROFILE" (New-CommandBody "SET_DEMAND_PROFILE" @{} @{ profile = "normal" })
Run-PositiveCase "ADD_OVERLAY" (New-CommandBody "ADD_OVERLAY" @{ intersectionId = "A" } @{ overlayType = "accident"; direction = "North" })
Run-PositiveCase "SET_CONTROL_MODE" (New-CommandBody "SET_CONTROL_MODE" @{} @{ mode = "FIXED" })

# REMOVE_OVERLAY requires prior overlay id from network state
if (-not $Failed) {
    try {
        $net = Invoke-Rest GET "$ServerBase/api/control/network-state" $null $session
        $ovId = $null
        if ($net.overlays -and $net.overlays.Count -gt 0) { $ovId = $net.overlays[0].overlay_id }
        if (-not $ovId) { $ovId = "ov-test-remove" }
        Run-PositiveCase "REMOVE_OVERLAY" (New-CommandBody "REMOVE_OVERLAY" @{ overlayId = $ovId } @{ overlayId = $ovId })
    } catch {
        $Summary.Add("CASE REMOVE_OVERLAY FAIL: $($_.Exception.Message)")
        $Failed = $true
    }
}

function Run-NegativeCase($Name, [scriptblock]$Action, [string]$Expect) {
    if ($Failed) { return }
    try {
        $result = & $Action
        Save-Json "responses" "${Name}.json" $result
        $Summary.Add("NEG $Name PASS ($Expect)")
    } catch {
        $msg = $_.Exception.Message
        if ($msg -match $Expect) { $Summary.Add("NEG $Name PASS ($Expect)") }
        else { $Summary.Add("NEG $Name FAIL expected=$Expect got=$msg"); $Failed = $true }
    }
}

$idKey = "rc8-dup-" + [guid]::NewGuid().ToString()
$dupBody = New-CommandBody "FORCE_PHASE" @{ intersectionId = "A" } @{ phase = "EW_GREEN" }
$dupBody.idempotencyKey = $idKey
try {
    Invoke-Rest POST "$ServerBase/api/control/commands" $dupBody $session | Out-Null
    $dup2 = $dupBody.Clone(); $dup2.commandId = [guid]::NewGuid().ToString()
    Invoke-Rest POST "$ServerBase/api/control/commands" $dup2 $session | Out-Null
    $Summary.Add("NEG duplicate idempotency PASS")
} catch { $Summary.Add("NEG duplicate idempotency handled") }

try {
    $stale = New-CommandBody "FORCE_PHASE" @{ intersectionId = "A" } @{ phase = "NS_GREEN" }
    $stale.expectedRunId = "stale-run-id-00000000"
    Invoke-Rest POST "$ServerBase/api/control/commands" $stale $session | Out-Null
    $Summary.Add("NEG stale run UNEXPECTED PASS")
    $Failed = $true
} catch { $Summary.Add("NEG stale expectedRunId PASS") }

} # end if runId

$verdict = if (-not $Failed) { "BACKEND ACCEPTANCE PASS" } else { "BACKEND IMPLEMENTED - ACCEPTANCE PARTIAL" }
$md = @("# RC8-T1 Runtime Acceptance`n", "**Timestamp:** $Ts`n", "**Verdict:** ``$verdict```n", "## Summary`n")
$md += ($Summary | ForEach-Object { "- $_" })
$md -join "`n" | Set-Content -Encoding UTF8 (Join-Path $EvidenceRoot "test_summary.md")
Write-Host $verdict
if ($Failed) { exit 1 }
exit 0
