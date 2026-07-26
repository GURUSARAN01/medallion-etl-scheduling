param(
    [string]$RepoRoot = "E:\Master thesis\medallion-etl-scheduling",
    [int]$TimeoutMinutes = 30,
    [int]$PollSeconds = 15,
    [int]$CooldownSeconds = 20
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$airflowDir = Join-Path $RepoRoot "airflow"
if (-not (Test-Path $airflowDir)) {
    throw "Airflow directory not found: $airflowDir"
}

Set-Location $airflowDir

# Counterbalanced continuation after the completed rounds 1--3.
$sequence = @(
    # Round 4
    [pscustomobject]@{ Round = 4; Layer = "bronze"; Strategy = "static";   DagId = "bronze_azure_static" },
    [pscustomobject]@{ Round = 4; Layer = "bronze"; Strategy = "xgboost"; DagId = "bronze_azure_xgboost" },
    [pscustomobject]@{ Round = 4; Layer = "bronze"; Strategy = "heft";     DagId = "bronze_azure_heft" },

    [pscustomobject]@{ Round = 4; Layer = "silver"; Strategy = "heft";     DagId = "silver_azure_heft" },
    [pscustomobject]@{ Round = 4; Layer = "silver"; Strategy = "static";   DagId = "silver_azure_static" },
    [pscustomobject]@{ Round = 4; Layer = "silver"; Strategy = "xgboost"; DagId = "silver_azure_xgboost" },

    [pscustomobject]@{ Round = 4; Layer = "gold";   Strategy = "xgboost"; DagId = "gold_azure_xgboost" },
    [pscustomobject]@{ Round = 4; Layer = "gold";   Strategy = "heft";     DagId = "gold_azure_heft" },
    [pscustomobject]@{ Round = 4; Layer = "gold";   Strategy = "static";   DagId = "gold_azure_static" },

    # Round 5
    [pscustomobject]@{ Round = 5; Layer = "bronze"; Strategy = "heft";     DagId = "bronze_azure_heft" },
    [pscustomobject]@{ Round = 5; Layer = "bronze"; Strategy = "static";   DagId = "bronze_azure_static" },
    [pscustomobject]@{ Round = 5; Layer = "bronze"; Strategy = "xgboost"; DagId = "bronze_azure_xgboost" },

    [pscustomobject]@{ Round = 5; Layer = "silver"; Strategy = "xgboost"; DagId = "silver_azure_xgboost" },
    [pscustomobject]@{ Round = 5; Layer = "silver"; Strategy = "heft";     DagId = "silver_azure_heft" },
    [pscustomobject]@{ Round = 5; Layer = "silver"; Strategy = "static";   DagId = "silver_azure_static" },

    [pscustomobject]@{ Round = 5; Layer = "gold";   Strategy = "static";   DagId = "gold_azure_static" },
    [pscustomobject]@{ Round = 5; Layer = "gold";   Strategy = "xgboost"; DagId = "gold_azure_xgboost" },
    [pscustomobject]@{ Round = 5; Layer = "gold";   Strategy = "heft";     DagId = "gold_azure_heft" }
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

    return Invoke-PostgresScalar -Sql $sql
}

Write-Host ""
Write-Host "Azure official rounds 4 and 5 preflight" -ForegroundColor Cyan

# Confirm that all nine benchmark DAGs are registered.
$dagIds = $sequence.DagId | Sort-Object -Unique
foreach ($dagId in $dagIds) {
    $count = Invoke-PostgresScalar -Sql @"
SELECT COUNT(*)
FROM dag
WHERE dag_id = '$dagId'
  AND is_active = TRUE;
"@

    if ([int]$count -ne 1) {
        throw "DAG is not active or not uniquely registered: $dagId"
    }

    Write-Host "DAG ready: $dagId"
}

# Do not overlap this extension with another active Azure benchmark run.
$quotedDagIds = ($dagIds | ForEach-Object { "'$_'" }) -join ","
$activeCount = Invoke-PostgresScalar -Sql @"
SELECT COUNT(*)
FROM dag_run
WHERE dag_id IN ($quotedDagIds)
  AND state IN ('queued', 'running');
"@

if ([int]$activeCount -gt 0) {
    throw "An Azure benchmark DAG is already queued or running. Wait for it to finish."
}

# Protect against accidentally generating more than one r4/r5 run.
foreach ($entry in $sequence) {
    $prefix = "azure_$($entry.Strategy)_$($entry.Layer)_official_r$($entry.Round)_"
    $existingCount = Invoke-PostgresScalar -Sql @"
SELECT COUNT(*)
FROM dag_run
WHERE dag_id = '$($entry.DagId)'
  AND run_id LIKE '$prefix%';
"@

    if ([int]$existingCount -gt 0) {
        throw "A run already exists for $prefix. Do not rerun the script until this is reviewed."
    }
}

# Unpause all benchmark DAGs before triggering the batch.
foreach ($dagId in $dagIds) {
    Invoke-Airflow -Arguments @("dags", "unpause", $dagId)
}

$outputDir = Join-Path $RepoRoot `
    "analysis\azure\official_n5"
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

$batchTimestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$manifestPath = Join-Path $outputDir `
    "azure_official_rounds_4_5_manifest_$batchTimestamp.csv"

Write-Host ""
Write-Host "Manifest will be written incrementally to:" -ForegroundColor Cyan
Write-Host $manifestPath
Write-Host ""

$completed = 0

foreach ($entry in $sequence) {
    $timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
    $runId = "azure_$($entry.Strategy)_$($entry.Layer)_official_r$($entry.Round)_$timestamp"

    Write-Host ""
    Write-Host (
        "[$($completed + 1)/$($sequence.Count)] " +
        "Round $($entry.Round) | $($entry.Layer) | $($entry.Strategy)"
    ) -ForegroundColor Green
    Write-Host "DAG:    $($entry.DagId)"
    Write-Host "Run ID: $runId"

    Invoke-Airflow -Arguments @(
        "dags",
        "trigger",
        "--run-id",
        $runId,
        $entry.DagId
    )

    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    $state = ""

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds $PollSeconds
        $state = Get-DagRunState -DagId $entry.DagId -RunId $runId
        Write-Host "State: $state"

        if ($state -eq "success") {
            break
        }

        if ($state -eq "failed") {
            throw "DAG run failed: $runId"
        }
    }

    if ($state -ne "success") {
        throw "DAG run timed out after $TimeoutMinutes minutes: $runId"
    }

    $detail = Invoke-PostgresScalar -Sql @"
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

    $parts = $detail -split "\|", 6
    if ($parts.Count -ne 6) {
        throw "Unexpected DAG-run detail format for $runId"
    }

    $row = [pscustomobject]@{
        Round        = $entry.Round
        Layer        = $entry.Layer
        Strategy     = $entry.Strategy
        DagId        = $parts[0]
        RunId        = $parts[1]
        State        = $parts[2]
        StartDateUtc = $parts[3]
        EndDateUtc   = $parts[4]
        DagRuntimeS  = $parts[5]
    }

    if (-not (Test-Path $manifestPath)) {
        $row | Export-Csv `
            -Path $manifestPath `
            -NoTypeInformation `
            -Encoding UTF8
    }
    else {
        $row | Export-Csv `
            -Path $manifestPath `
            -NoTypeInformation `
            -Encoding UTF8 `
            -Append
    }

    $completed += 1
    Write-Host "Completed successfully: $runId" -ForegroundColor Green

    if ($completed -lt $sequence.Count) {
        Start-Sleep -Seconds $CooldownSeconds
    }
}

Write-Host ""
Write-Host "All 18 official round-4 and round-5 runs completed." `
    -ForegroundColor Green
Write-Host "Manifest: $manifestPath"

Import-Csv $manifestPath |
    Format-Table Round, Layer, Strategy, State, DagRuntimeS -AutoSize
