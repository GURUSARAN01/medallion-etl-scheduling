# Azure Airflow Integration Setup

## Architecture

Airflow remains the scheduling system. Each Airflow task submits one
parameterised notebook run to a shared existing Azure Databricks cluster.

The Bronze DAG retains the original dependencies and uses:

- `max_active_tasks=2`
- Static equal priorities
- Azure-pilot HEFT priorities
- Fixed local XGBoost priorities

## 1. Install the Databricks provider

The local environment uses Airflow 2.9. Pin the provider to version 7.0.0.

In `airflow/docker-compose.yml`, add this under the shared Airflow environment:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: "apache-airflow-providers-databricks==7.0.0"
```

Rebuild/restart from PowerShell:

```powershell
cd "E:\Master thesis\medallion-etl-scheduling\airflow"
docker compose down
docker compose up airflow-init
docker compose up -d
```

Verify:

```powershell
docker compose exec airflow-webserver `
  pip show apache-airflow-providers-databricks
```

## 2. Import the Databricks notebook

Import:

```text
azure/notebooks/03_bronze_task.py
```

Suggested workspace path:

```text
/Shared/03_bronze_task
```

## 3. Create a small classic Databricks cluster

Use one existing cluster for the Bronze integration test.

Rules:

- Smallest available general-purpose node
- Single-node mode when available
- Photon off
- Autoscaling off
- Auto-termination 10 minutes
- Start it only for a test session

Copy its cluster ID from the Databricks cluster configuration page.

## 4. Create a Databricks personal access token

In Databricks:

```text
User settings -> Developer -> Access tokens -> Generate new token
```

Use a short lifetime that covers the experiment window.

Do not commit the token to Git.

## 5. Create the Airflow connection

In Airflow:

```text
Admin -> Connections -> Add
```

Use:

```text
Connection Id: databricks_azure
Connection Type: Databricks
Host: https://<your-workspace-host>
Login: leave empty
Password: <your Databricks PAT>
```

## 6. Create Airflow variables

In Airflow:

```text
Admin -> Variables
```

Create:

```text
databricks_azure_cluster_id = <cluster-id>
databricks_bronze_notebook_path = /Shared/03_bronze_task
databricks_target_catalog = dbw_thesis_etl_scheduling
databricks_target_schema = thesis_etl_scheduling
```

## 7. Copy the DAG files

Copy these files to `airflow/dags/`:

```text
azure_bronze_common.py
bronze_azure_static_dag.py
bronze_azure_heft_dag.py
bronze_azure_xgb_dag.py
```

Check import errors:

```powershell
cd "E:\Master thesis\medallion-etl-scheduling\airflow"

docker compose exec airflow-webserver `
  airflow dags list-import-errors
```

## 8. First integration test

Start the small Databricks cluster manually.

Trigger only:

```text
bronze_azure_static
```

Confirm:

- Six Databricks task runs succeed.
- No more than two Airflow tasks run concurrently.
- Six new metric rows appear for the Airflow DAG run ID.
- The cluster is terminated immediately after inspection.

Do not trigger HEFT or XGBoost until the static integration test succeeds.
