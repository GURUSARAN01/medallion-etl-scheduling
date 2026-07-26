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

function Invoke-Airflow {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    # Some Azure SDK packages emit harmless Python SyntaxWarning messages
    # to stderr. Windows PowerShell can promote those stderr records to
    # NativeCommandError when the script uses ErrorActionPreference=Stop.
    # Temporarily allow native stderr, suppress Python warnings inside the
    # Airflow container, and still fail correctly on a non-zero exit code.
    $previousErrorActionPreference = $ErrorActionPreference

    try {
        $ErrorActionPreference = "Continue"

        $commandOutput = & docker compose exec -T `
            -e PYTHONWARNINGS=ignore `
            airflow-scheduler `
            airflow @Arguments 2>&1

        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

    foreach ($line in $commandOutput) {
        Write-Host $line
    }

    if ($exitCode -ne 0) {
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

function Get-DagRunDetail {
    param(
        [Parameter(Mandatory = $true)][string]$DagId,
        [Parameter(Mandatory = $true)][string]$RunId
    )

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

function Wait-ForDagRun {
    param(
        [Parameter(Mandatory = $true)][string]$DagId,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][int]$TimeoutMinutes,
        [Parameter(Mandatory = $true)][int]$PollSeconds
    )

    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    $state = ""

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds $PollSeconds
        $state = Get-DagRunState -DagId $DagId -RunId $RunId
        Write-Host "State: $state"

        if ($state -eq "success") {
            return Get-DagRunDetail -DagId $DagId -RunId $RunId
        }

        if ($state -eq "failed") {
            throw "DAG run failed: $RunId"
        }
    }

    throw "DAG run timed out after $TimeoutMinutes minutes: $RunId"
}

function Add-ManifestRow {
    param(
        [Parameter(Mandatory = $true)][string]$ManifestPath,
        [Parameter(Mandatory = $true)][int]$Round,
        [Parameter(Mandatory = $true)][int]$Concurrency,
        [Parameter(Mandatory = $true)][pscustomobject]$Detail
    )

    $row = [pscustomobject]@{
        Round        = $Round
        Concurrency  = $Concurrency
        DagId        = $Detail.DagId
        RunId        = $Detail.RunId
        State        = $Detail.State
        StartDateUtc = $Detail.StartDateUtc
        EndDateUtc   = $Detail.EndDateUtc
        DagRuntimeS  = $Detail.DagRuntimeS
    }

    if (Test-Path $ManifestPath) {
        $row | Export-Csv `
            -Path $ManifestPath `
            -NoTypeInformation `
            -Encoding UTF8 `
            -Append
    }
    else {
        $row | Export-Csv `
            -Path $ManifestPath `
            -NoTypeInformation `
            -Encoding UTF8
    }
}

# Near-counterbalanced five-round sequence. Every condition appears five
# times, and first/last positions are distributed as evenly as possible.
$sequence = @(
    [pscustomobject]@{ Round = 1; Concurrency = 1; DagId = "gold_azure_concurrency_c1" },
    [pscustomobject]@{ Round = 1; Concurrency = 2; DagId = "gold_azure_concurrency_c2" },
    [pscustomobject]@{ Round = 1; Concurrency = 3; DagId = "gold_azure_concurrency_c3" },

    [pscustomobject]@{ Round = 2; Concurrency = 2; DagId = "gold_azure_concurrency_c2" },
    [pscustomobject]@{ Round = 2; Concurrency = 3; DagId = "gold_azure_concurrency_c3" },
    [pscustomobject]@{ Round = 2; Concurrency = 1; DagId = "gold_azure_concurrency_c1" },

    [pscustomobject]@{ Round = 3; Concurrency = 3; DagId = "gold_azure_concurrency_c3" },
    [pscustomobject]@{ Round = 3; Concurrency = 1; DagId = "gold_azure_concurrency_c1" },
    [pscustomobject]@{ Round = 3; Concurrency = 2; DagId = "gold_azure_concurrency_c2" },

    [pscustomobject]@{ Round = 4; Concurrency = 1; DagId = "gold_azure_concurrency_c1" },
    [pscustomobject]@{ Round = 4; Concurrency = 3; DagId = "gold_azure_concurrency_c3" },
    [pscustomobject]@{ Round = 4; Concurrency = 2; DagId = "gold_azure_concurrency_c2" },

    [pscustomobject]@{ Round = 5; Concurrency = 2; DagId = "gold_azure_concurrency_c2" },
    [pscustomobject]@{ Round = 5; Concurrency = 1; DagId = "gold_azure_concurrency_c1" },
    [pscustomobject]@{ Round = 5; Concurrency = 3; DagId = "gold_azure_concurrency_c3" }
)

$dagIds = $sequence.DagId | Sort-Object -Unique

Write-Host ""
Write-Host "Gold concurrency-sensitivity preflight" -ForegroundColor Cyan

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

    Invoke-Airflow -Arguments @("dags", "unpause", $dagId)
    Write-Host "DAG ready: $dagId"
}

# Block overlap with any known Azure benchmark or mechanism DAG.
$activeRows = Invoke-PostgresScalar -Sql @"
SELECT dag_id || '|' || run_id || '|' || state
FROM dag_run
WHERE dag_id LIKE '%azure%'
  AND state IN ('queued', 'running')
ORDER BY execution_date;
"@

if ($activeRows) {
    throw "Another Azure DAG is active:`n$activeRows"
}

$outputDir = Join-Path $RepoRoot `
    "analysis\azure\mechanisms\gold_concurrency"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$manifest = Join-Path $outputDir `
    "azure_gold_concurrency_manifest_$stamp.csv"

$completed = 0

foreach ($entry in $sequence) {
    $prefix = (
        "azure_gold_concurrency_c$($entry.Concurrency)_" +
        "r$($entry.Round)_"
    )

    $matchingRuns = Invoke-PostgresScalar -Sql @"
SELECT run_id || '|' || state
FROM dag_run
WHERE dag_id = '$($entry.DagId)'
  AND run_id LIKE '$prefix%'
ORDER BY execution_date;
"@

    $detail = $null

    if ($matchingRuns) {
        $lines = @(
            $matchingRuns -split "`r?`n" |
                Where-Object { $_.Trim() }
        )

        if ($lines.Count -gt 1) {
            throw "Multiple runs already exist for $prefix. Review before continuing."
        }

        $existingParts = $lines[0] -split "\|", 2
        $existingRunId = $existingParts[0]
        $existingState = $existingParts[1]

        Write-Host ""
        Write-Host (
            "[$($completed + 1)/$($sequence.Count)] " +
            "Existing concurrency run | round $($entry.Round) | " +
            "c$($entry.Concurrency)"
        ) -ForegroundColor Yellow
        Write-Host "Run ID: $existingRunId"
        Write-Host "State:  $existingState"

        if ($existingState -eq "success") {
            $detail = Get-DagRunDetail `
                -DagId $entry.DagId `
                -RunId $existingRunId
            Write-Host "Reusing successful run." -ForegroundColor Green
        }
        elseif ($existingState -in @("queued", "running")) {
            $detail = Wait-ForDagRun `
                -DagId $entry.DagId `
                -RunId $existingRunId `
                -TimeoutMinutes $TimeoutMinutes `
                -PollSeconds $PollSeconds
        }
        else {
            throw (
                "Existing run $existingRunId has state '$existingState'. " +
                "Review it before continuing."
            )
        }
    }
    else {
        $runStamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
        $runId = "$prefix$runStamp"

        Write-Host ""
        Write-Host (
            "[$($completed + 1)/$($sequence.Count)] " +
            "Gold concurrency | round $($entry.Round) | " +
            "c$($entry.Concurrency)"
        ) -ForegroundColor Green
        Write-Host "Run ID: $runId"

        Invoke-Airflow -Arguments @(
            "dags",
            "trigger",
            "--run-id",
            $runId,
            $entry.DagId
        )

        $detail = Wait-ForDagRun `
            -DagId $entry.DagId `
            -RunId $runId `
            -TimeoutMinutes $TimeoutMinutes `
            -PollSeconds $PollSeconds
    }

    Add-ManifestRow `
        -ManifestPath $manifest `
        -Round $entry.Round `
        -Concurrency $entry.Concurrency `
        -Detail $detail

    $completed += 1
    Write-Host "Recorded: $($detail.RunId)" -ForegroundColor Green

    if ($completed -lt $sequence.Count) {
        Start-Sleep -Seconds $CooldownSeconds
    }
}

Write-Host ""
Write-Host (
    "All 15 Gold concurrency-sensitivity runs are present and successful."
) -ForegroundColor Green
Write-Host "Manifest: $manifest"

Import-Csv $manifest |
    Format-Table Round, Concurrency, State, DagRuntimeS -AutoSize
