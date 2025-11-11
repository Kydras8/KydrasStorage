param([switch]$Coverage=[switch]::Present)
$ErrorActionPreference = "Stop"
$cfg="Release"
$trx="TestResults/results.trx"
$covdir="TestResults/Coverage"
$collect = @()
if($Coverage){ $collect = @("--collect","XPlat Code Coverage","--results-directory",$covdir) }
Write-Host "[test] running NUnit"
dotnet test -c $cfg --logger "trx;LogFileName=$trx" $collect
if($Coverage){
  Write-Host "[test] coverage saved under $covdir"
}
