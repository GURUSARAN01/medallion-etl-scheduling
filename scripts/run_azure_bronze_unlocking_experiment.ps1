param(
    [string]$RepoRoot = "E:\Master thesis\medallion-etl-scheduling",
    [int]$TimeoutMinutes = 30,
    [int]$PollSeconds = 15,
    [int]$CooldownSeconds = 20
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location (Join-Path $RepoRoot "airflow")


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

    return Invoke-PostgresScalar -Sql @"
SELECT COALESCE(state, '')
FROM dag_run
WHERE dag_id = '$DagId'
  AND run_id = '$RunId'
LIMIT 1;
"@
}

function Confirm-DagsReady {
    param([Parameter(Mandatory = $true)][string[]]$DagIds)

    foreach ($dagId in ($DagIds | Sort-Object -Unique)) {
        $count = Invoke-PostgresScalar -Sql @"
SELECT COUNT(*)
FROM dag
WHERE dag_id = '$dagId'
  AND is_active = TRUE;
"@

        if ([int]$count -ne 1) {
            throw "DAG is not active or not uniquely registered: $dagId"
        }

        Invoke-Airflow -Arguments @("dags", "unpause", $dagId)
        Write-Host "DAG ready: $dagId"
    }
}

function Confirm-NoActiveRuns {
    param([Parameter(Mandatory = $true)][string[]]$DagIds)

    $quoted = ($DagIds | Sort-Object -Unique |
        ForEach-Object { "'$_'" }) -join ","

    $count = Invoke-PostgresScalar -Sql @"
SELECT COUNT(*)
FROM dag_run
WHERE dag_id IN ($quoted)
  AND state IN ('queued', 'running');
"@

    if ([int]$count -gt 0) {
        throw "An extension DAG is already queued or running."
    }
}

function Invoke-SequentialDagRun {
    param(
        [Parameter(Mandatory = $true)][string]$DagId,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][int]$TimeoutMinutes,
        [Parameter(Mandatory = $true)][int]$PollSeconds
    )

    Invoke-Airflow -Arguments @(
        "dags", "trigger",
        "--run-id", $RunId,
        $DagId
    ) | Out-Host

    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    $state = ""

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds $PollSeconds
        $state = Get-DagRunState -DagId $DagId -RunId $RunId
        Write-Host "State: $state"

        if ($state -eq "success") {
            break
        }

        if ($state -eq "failed") {
            throw "DAG run failed: $RunId"
        }
    }

    if ($state -ne "success") {
        throw "DAG run timed out: $RunId"
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
WHERE dag_id = '$DagId'
  AND run_id = '$RunId'
LIMIT 1;
"@

    $parts = $detail -split "\|", 6
    if ($parts.Count -ne 6) {
        throw "Unexpected DAG-run detail format for $RunId"
    }

    return [pscustomobject]@{
        DagId        = $parts[0]
        RunId        = $parts[1]
        State        = $parts[2]
        StartDateUtc = $parts[3]
        EndDateUtc   = $parts[4]
        DagRuntimeS  = $parts[5]
    }
}


# Five repetitions per controlled ordering.
$sequence = @(
    [pscustomobject]@{ Round = 1; Variant = "early";   DagId = "bronze_azure_unlock_early" },
    [pscustomobject]@{ Round = 1; Variant = "delayed"; DagId = "bronze_azure_unlock_delayed" },

    [pscustomobject]@{ Round = 2; Variant = "delayed"; DagId = "bronze_azure_unlock_delayed" },
    [pscustomobject]@{ Round = 2; Variant = "early";   DagId = "bronze_azure_unlock_early" },

    [pscustomobject]@{ Round = 3; Variant = "early";   DagId = "bronze_azure_unlock_early" },
    [pscustomobject]@{ Round = 3; Variant = "delayed"; DagId = "bronze_azure_unlock_delayed" },

    [pscustomobject]@{ Round = 4; Variant = "delayed"; DagId = "bronze_azure_unlock_delayed" },
    [pscustomobject]@{ Round = 4; Variant = "early";   DagId = "bronze_azure_unlock_early" },

    [pscustomobject]@{ Round = 5; Variant = "early";   DagId = "bronze_azure_unlock_early" },
    [pscustomobject]@{ Round = 5; Variant = "delayed"; DagId = "bronze_azure_unlock_delayed" }
)

$dagIds = $sequence.DagId | Sort-Object -Unique
Confirm-DagsReady -DagIds $dagIds
Confirm-NoActiveRuns -DagIds $dagIds

foreach ($entry in $sequence) {
    $prefix = "azure_bronze_unlock_$($entry.Variant)_r$($entry.Round)_"
    $existing = Invoke-PostgresScalar -Sql @"
SELECT COUNT(*)
FROM dag_run
WHERE dag_id = '$($entry.DagId)'
  AND run_id LIKE '$prefix%';
"@
    if ([int]$existing -gt 0) {
        throw "A run already exists for $prefix"
    }
}

$outputDir = Join-Path $RepoRoot `
    "analysis\azure\mechanisms\bronze_unlocking"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$manifest = Join-Path $outputDir `
    "azure_bronze_unlocking_manifest_$stamp.csv"

$completed = 0
foreach ($entry in $sequence) {
    $runStamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
    $runId = "azure_bronze_unlock_$($entry.Variant)_r$($entry.Round)_$runStamp"

    Write-Host ""
    Write-Host (
        "[$($completed + 1)/$($sequence.Count)] " +
        "Bronze unlocking | round $($entry.Round) | $($entry.Variant)"
    ) -ForegroundColor Green
    Write-Host "Run ID: $runId"

    $detail = Invoke-SequentialDagRun `
        -DagId $entry.DagId `
        -RunId $runId `
        -TimeoutMinutes $TimeoutMinutes `
        -PollSeconds $PollSeconds

    $row = [pscustomobject]@{
        Round        = $entry.Round
        Variant      = $entry.Variant
        DagId        = $detail.DagId
        RunId        = $detail.RunId
        State        = $detail.State
        StartDateUtc = $detail.StartDateUtc
        EndDateUtc   = $detail.EndDateUtc
        DagRuntimeS  = $detail.DagRuntimeS
    }

    if (Test-Path $manifest) {
        $row | Export-Csv $manifest -NoTypeInformation -Encoding UTF8 -Append
    }
    else {
        $row | Export-Csv $manifest -NoTypeInformation -Encoding UTF8
    }

    $completed += 1
    if ($completed -lt $sequence.Count) {
        Start-Sleep -Seconds $CooldownSeconds
    }
}

Write-Host ""
Write-Host "All 10 Bronze unlocking runs completed." -ForegroundColor Green
Write-Host "Manifest: $manifest"
