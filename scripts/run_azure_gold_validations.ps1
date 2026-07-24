Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location "E:\Master thesis\medallion-etl-scheduling\airflow"

$sequence = @(
    [pscustomobject]@{ Strategy = "static";  DagId = "gold_azure_static" },
    [pscustomobject]@{ Strategy = "heft";    DagId = "gold_azure_heft" },
    [pscustomobject]@{ Strategy = "xgboost"; DagId = "gold_azure_xgboost" }
)

function Invoke-Airflow {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    & docker compose exec -T airflow-scheduler airflow @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Airflow command failed: airflow $($Arguments -join ' ')"
    }
}

function Get-DagRunState {
    param(
        [Parameter(Mandatory = $true)][string]$DagId,
        [Parameter(Mandatory = $true)][string]$RunId
    )

    $sql = @"
SELECT COALESCE(state, '')
FROM dag_run
WHERE dag_id = '$DagId'
  AND run_id = '$RunId'
LIMIT 1;
"@

    $result = & docker compose exec -T postgres `
        psql -U airflow -d airflow -t -A -c $sql

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to query Airflow metadata database."
    }

    return ($result | Out-String).Trim()
}

foreach ($entry in $sequence) {
    Invoke-Airflow -Arguments @("dags", "unpause", $entry.DagId)

    $timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
    $runId = "azure_$($entry.Strategy)_gold_validation_$timestamp"

    Write-Host ""
    Write-Host "Triggering $($entry.DagId)" -ForegroundColor Green
    Write-Host "Run ID: $runId"

    Invoke-Airflow -Arguments @(
        "dags", "trigger",
        "--run-id", $runId,
        $entry.DagId
    )

    $deadline = (Get-Date).AddMinutes(30)
    $state = ""

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 15
        $state = Get-DagRunState -DagId $entry.DagId -RunId $runId
        Write-Host "State: $state"

        if ($state -eq "success") {
            break
        }

        if ($state -eq "failed") {
            throw "Validation run failed: $runId"
        }
    }

    if ($state -ne "success") {
        throw "Validation run timed out: $runId"
    }

    Write-Host "Completed: $runId" -ForegroundColor Green
    Start-Sleep -Seconds 20
}

Write-Host ""
Write-Host "All three Gold validation runs completed." -ForegroundColor Green
