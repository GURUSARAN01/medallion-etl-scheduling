Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$airflowDir = "E:\Master thesis\medallion-etl-scheduling\airflow"
Set-Location $airflowDir

$pollSeconds = 15
$timeoutMinutes = 30

$sequence = @(
    [pscustomobject]@{ Round = 1; Strategy = "static";   DagId = "bronze_azure_static" },
    [pscustomobject]@{ Round = 1; Strategy = "heft";     DagId = "bronze_azure_heft" },
    [pscustomobject]@{ Round = 1; Strategy = "xgboost";  DagId = "bronze_azure_xgboost" },

    [pscustomobject]@{ Round = 2; Strategy = "heft";     DagId = "bronze_azure_heft" },
    [pscustomobject]@{ Round = 2; Strategy = "xgboost";  DagId = "bronze_azure_xgboost" },
    [pscustomobject]@{ Round = 2; Strategy = "static";   DagId = "bronze_azure_static" },

    [pscustomobject]@{ Round = 3; Strategy = "xgboost";  DagId = "bronze_azure_xgboost" },
    [pscustomobject]@{ Round = 3; Strategy = "static";   DagId = "bronze_azure_static" },
    [pscustomobject]@{ Round = 3; Strategy = "heft";     DagId = "bronze_azure_heft" }
)

function Invoke-Airflow {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    & docker compose exec -T airflow-scheduler airflow @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Airflow command failed: airflow $($Arguments -join ' ')"
    }
}

function Invoke-PostgresScalar {
    param([Parameter(Mandatory = $true)][string]$Sql)

    $result = & docker compose exec -T postgres `
        psql -U airflow -d airflow -t -A -c $Sql

    if ($LASTEXITCODE -ne 0) {
        throw "PostgreSQL query failed."
    }

    return ($result | Out-String).Trim()
}

Write-Host "Checking Airflow containers..." -ForegroundColor Cyan
& docker compose ps
if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose services are not available."
}

Write-Host "Unpausing the three manual-schedule DAGs..." -ForegroundColor Cyan
Invoke-Airflow -Arguments @("dags", "unpause", "bronze_azure_static")
Invoke-Airflow -Arguments @("dags", "unpause", "bronze_azure_heft")
Invoke-Airflow -Arguments @("dags", "unpause", "bronze_azure_xgboost")

$results = New-Object System.Collections.Generic.List[object]

foreach ($entry in $sequence) {
    $timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
    $runId = "azure_$($entry.Strategy)_bronze_official_r$($entry.Round)_$timestamp"

    Write-Host ""
    Write-Host "Triggering $($entry.DagId)" -ForegroundColor Green
    Write-Host "Run ID: $runId"

    Invoke-Airflow -Arguments @(
        "dags", "trigger",
        "--run-id", $runId,
        $entry.DagId
    )

    $deadline = (Get-Date).AddMinutes($timeoutMinutes)
    $state = ""

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds $pollSeconds

        $sql = @"
SELECT COALESCE(state, '')
FROM dag_run
WHERE dag_id = '$($entry.DagId)'
  AND run_id = '$runId'
LIMIT 1;
"@

        $state = Invoke-PostgresScalar -Sql $sql

        if ([string]::IsNullOrWhiteSpace($state)) {
            Write-Host "State: not yet recorded"
            continue
        }

        Write-Host "State: $state"

        if ($state -eq "success") {
            break
        }

        if ($state -in @("failed", "upstream_failed")) {
            throw "DAG run failed: $runId"
        }
    }

    if ($state -ne "success") {
        throw "Timed out after $timeoutMinutes minutes: $runId"
    }

    $detailSql = @"
SELECT
    dag_id || '|' ||
    run_id || '|' ||
    state || '|' ||
    COALESCE(start_date::text, '') || '|' ||
    COALESCE(end_date::text, '') || '|' ||
    COALESCE(
        ROUND(EXTRACT(EPOCH FROM (end_date - start_date))::numeric, 3)::text,
        ''
    )
FROM dag_run
WHERE dag_id = '$($entry.DagId)'
  AND run_id = '$runId'
LIMIT 1;
"@

    $detail = Invoke-PostgresScalar -Sql $detailSql
    $parts = $detail -split "\|", 6

    $results.Add([pscustomobject]@{
        Round        = $entry.Round
        Strategy     = $entry.Strategy
        DagId        = $parts[0]
        RunId        = $parts[1]
        State        = $parts[2]
        StartDateUtc = $parts[3]
        EndDateUtc   = $parts[4]
        DagRuntimeS  = $parts[5]
    })

    Write-Host "Completed: $runId" -ForegroundColor Green
    Start-Sleep -Seconds 20
}

$outputDir = "E:\Master thesis\medallion-etl-scheduling\analysis"
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$outputPath = Join-Path $outputDir (
    "azure_bronze_official_manifest_" +
    (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") +
    ".csv"
)

$results | Export-Csv -Path $outputPath -NoTypeInformation -Encoding UTF8

Write-Host ""
Write-Host "All nine official Bronze runs completed." -ForegroundColor Green
Write-Host "Manifest: $outputPath"
$results | Format-Table -AutoSize

Write-Host ""
Write-Host "Terminate the Databricks compute after exporting the Databricks metrics." -ForegroundColor Yellow