param([switch]$CI)
$ErrorActionPreference = "Stop"
Write-Host "[build] restore"
dotnet restore
Write-Host "[build] build (Release)"
dotnet build -c Release --no-restore
if(-not $CI){ Write-Host "[build] done" }
