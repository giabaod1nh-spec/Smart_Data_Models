# Demo scenarios for hybrid demand / overlays
# Requires: traci_runner Control API on CONTROL_API_PORT (default 9090)

$ErrorActionPreference = "Stop"
$Base = "http://127.0.0.1:9090"

Write-Host "Health..."
Invoke-RestMethod "$Base/health" | ConvertTo-Json -Compress

Write-Host "Demand: morning_peak"
Invoke-RestMethod -Method POST -Uri "$Base/demand-profile" -ContentType "application/json" -Body '{"profile":"morning_peak"}'

Start-Sleep -Seconds 2

Write-Host "Overlay: accident B West incoming_approach"
Invoke-RestMethod -Method POST -Uri "$Base/overlays" -ContentType "application/json" -Body '{"overlay_type":"accident","intersection_id":"B","direction":"West","segment_role":"incoming_approach"}'

Start-Sleep -Seconds 2

Write-Host "Network state"
Invoke-RestMethod "$Base/network-state" | ConvertTo-Json -Depth 6

Write-Host "Intersection A (should not be incident_active)"
Invoke-RestMethod "$Base/intersections/A/state" | ConvertTo-Json -Depth 6

Write-Host "Intersection B"
Invoke-RestMethod "$Base/intersections/B/state" | ConvertTo-Json -Depth 6

Write-Host "Stats"
Invoke-RestMethod "$Base/stats" | ConvertTo-Json -Depth 4

Write-Host "Done."
