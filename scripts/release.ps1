param(
  [Parameter(Mandatory=$true)][string]$Version,
  [string]$Title = "Release $Version",
  [string]$Notes = "Automated release",
  [string]$Artifacts = ".\artifacts"
)
$ErrorActionPreference="Stop"
if(-not (Test-Path $Artifacts)){ ./scripts/pack.ps1; $Artifacts = ".\artifacts" }
kyrelease $Version -Title $Title -Notes $Notes -Artifacts $Artifacts
