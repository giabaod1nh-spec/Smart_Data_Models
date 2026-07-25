# Phase 3.8.1A — netconvert + TLS re-apply + TLS/geometry gates.
# Usage (from Visualize/): .\tools\rebuild_phase38_net.ps1
$ErrorActionPreference = "Stop"
$Root = Split-Path $PSScriptRoot -Parent
Set-Location $Root
$env:PATH = "$env:SUMO_HOME\bin;$env:PATH"

Write-Host "== 3.8.1A netconvert =="
& "$PSScriptRoot\regen_net.ps1"
if ($LASTEXITCODE -ne 0) { throw "netconvert failed" }

Write-Host "== TLS re-apply =="
python "$PSScriptRoot\reapply_tls.py"
if ($LASTEXITCODE -ne 0) { throw "TLS re-apply failed" }

Write-Host "== TLS gate =="
$Freeze = Join-Path $Root "artifacts\phase3.8\phase38_pre_geometry_audit.json"
if (-not (Test-Path $Freeze)) {
    throw "Missing freeze snapshot: $Freeze (generate it before rebuilding the network)"
}
python "$PSScriptRoot\verify_tls_gate.py" --freeze "$Freeze"
if ($LASTEXITCODE -ne 0) { throw "TLS gate failed - stop before detectors" }

Write-Host "== Geometry / ID / FF gate =="
python "$PSScriptRoot\verify_geometry_gate.py" --json-out "$Root\artifacts\phase3.8\geometry_gate.json"
if ($LASTEXITCODE -ne 0) { throw "Geometry gate failed" }

Write-Host "Phase 3.8.1A PASS"
