$ErrorActionPreference = "Stop"

# Go to Airflow compose folder
Set-Location "E:\Master thesis\medallion-etl-scheduling\airflow"

# Change this if you want fewer/more extra repetitions.
# Since you already have 5 runs, this creates repetitions 6 to 10.
$StartRep = 6
$EndRep = 10

# Use all three strategies for stronger statistics.
# If you only want XGBoost, change this to: @("xgboost")
$StrategiesToRun = @("static", "heft", "xgboost")

$DagMap = @{
    "static" = @{
        "bronze" = "bronze_ingestion"
        "silver" = "silver_transformation"
        "gold"   = "gold_aggregation"
    }
    "heft" = @{
        "bronze" = "bronze_ingestion_heft"
        "silver" = "silver_transformation_heft"
        "gold"   = "gold_aggregation_heft"
    }
    "xgboost" = @{
        "bronze" = "bronze_ingestion_xgb"
        "silver" = "silver_transformation_xgb"
        "gold"   = "gold_aggregation_xgb"
    }
}

function Get-DagState {
    param (
        [string]$DagId,
        [string]$RunId
    )

    $Query = "SELECT state FROM dag_run WHERE dag_id = '$DagId' AND run_id = '$RunId' ORDER BY execution_date DESC LIMIT 1;"

    $State = docker compose exec -T postgres psql -U airflow -d airflow -t -A -c $Query

    return $State.Trim()
}

function Wait-DagFinished {
    param (
        [string]$DagId,
        [string]$RunId
    )

    Write-Host "Waiting for $DagId / $RunId ..."

    while ($true) {
        $State = Get-DagState -DagId $DagId -RunId $RunId

        if ($State -eq "success") {
            Write-Host "SUCCESS: $DagId / $RunId"
            break
        }

        if ($State -eq "failed") {
            throw "FAILED: $DagId / $RunId"
        }

        if ($State -eq "upstream_failed") {
            throw "UPSTREAM FAILED: $DagId / $RunId"
        }

        Write-Host "Current state: $State"
        Start-Sleep -Seconds 5
    }
}

function Trigger-Dag {
    param (
        [string]$DagId,
        [string]$RunId
    )

    Write-Host ""
    Write-Host "Triggering $DagId with run_id=$RunId"

    docker compose exec -T airflow-webserver airflow dags trigger --run-id $RunId $DagId | Out-Host
}

foreach ($Rep in $StartRep..$EndRep) {
    foreach ($Strategy in $StrategiesToRun) {
        $Timestamp = Get-Date -Format "yyyyMMddHHmmss"

        Write-Host ""
        Write-Host "======================================================"
        Write-Host "Starting repetition $Rep for strategy: $Strategy"
        Write-Host "======================================================"

        $BronzeDag = $DagMap[$Strategy]["bronze"]
        $SilverDag = $DagMap[$Strategy]["silver"]
        $GoldDag   = $DagMap[$Strategy]["gold"]

        $BronzeRunId = "auto_${Strategy}_rep_${Rep}_bronze_${Timestamp}"
        $SilverRunId = "auto_${Strategy}_rep_${Rep}_silver_${Timestamp}"
        $GoldRunId   = "auto_${Strategy}_rep_${Rep}_gold_${Timestamp}"

        Trigger-Dag -DagId $BronzeDag -RunId $BronzeRunId
        Wait-DagFinished -DagId $BronzeDag -RunId $BronzeRunId

        Trigger-Dag -DagId $SilverDag -RunId $SilverRunId
        Wait-DagFinished -DagId $SilverDag -RunId $SilverRunId

        Trigger-Dag -DagId $GoldDag -RunId $GoldRunId
        Wait-DagFinished -DagId $GoldDag -RunId $GoldRunId

        Write-Host "Completed repetition $Rep for strategy: $Strategy"
    }
}

Write-Host ""
Write-Host "All requested benchmark repetitions completed."

Write-Host ""
Write-Host "Final row counts:"
docker compose exec -T postgres psql -U airflow -d airflow -P pager=off -c "SELECT strategy, layer, COUNT(*) AS rows FROM etl_task_run_metrics GROUP BY strategy, layer ORDER BY strategy, layer;"