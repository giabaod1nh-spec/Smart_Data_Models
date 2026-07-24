# Regenerate intersection.net.xml from nod+edg (ADR-003).
# Usage: from Visualize/  .\tools\regen_net.ps1
$ErrorActionPreference = "Stop"
$Root = Split-Path $PSScriptRoot -Parent
$Assets = Join-Path $Root "Visualize"
$env:PATH = "$env:SUMO_HOME\bin;$env:PATH"
netconvert `
  --node-files="$Assets\intersection.nod.xml" `
  --edge-files="$Assets\intersection.edg.xml" `
  --output-file="$Assets\intersection.net.xml" `
  --no-turnarounds.except-deadend true
Write-Host "netconvert OK -> $Assets\intersection.net.xml"
