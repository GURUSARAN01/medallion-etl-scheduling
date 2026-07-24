Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location "E:\Master thesis\medallion-etl-scheduling\airflow"
$runId = "azure_static_silver_pilot_" + (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
docker compose exec airflow-scheduler airflow dags unpause silver_azure_static_pilot
if ($LASTEXITCODE -ne 0) { throw "Failed to unpause DAG." }
docker compose exec airflow-scheduler airflow dags trigger --run-id $runId silver_azure_static_pilot
if ($LASTEXITCODE -ne 0) { throw "Failed to trigger DAG." }
Write-Host "Run ID: $runId" -ForegroundColor Green
