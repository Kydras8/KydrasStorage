$ErrorActionPreference="Stop"
$out = "artifacts"
Remove-Item $out -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force $out | Out-Null
dotnet build -c Release
$bins = Get-ChildItem -Recurse -Filter "*.dll" | Where-Object { $_.FullName -match "bin\\Release" }
$zip = Join-Path $out ("KydrasStorage-bin-" + (Get-Date -Format "yyyyMMdd-HHmm") + ".zip")
if(Test-Path $zip){ Remove-Item $zip -Force }
Compress-Archive -Path $bins.FullName -DestinationPath $zip -Force
Write-Host "[pack] $zip"
