Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location "E:\Master thesis\medallion-etl-scheduling\airflow"

$runId = "azure_static_gold_pilot_" + `
    (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")

docker compose exec airflow-scheduler `
    airflow dags unpause gold_azure_static_pilot

if ($LASTEXITCODE -ne 0) {
    throw "Failed to unpause gold_azure_static_pilot."
}

docker compose exec airflow-scheduler `
    airflow dags trigger `
    --run-id $runId `
    gold_azure_static_pilot

if ($LASTEXITCODE -ne 0) {
    throw "Failed to trigger gold_azure_static_pilot."
}

Write-Host ""
Write-Host "Gold Static pilot triggered." -ForegroundColor Green
Write-Host "Run ID: $runId"
Write-Host "Monitor it in the Airflow Grid view."
