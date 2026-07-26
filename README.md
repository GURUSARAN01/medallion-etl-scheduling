# Medallion ETL Scheduling

Experimental repository for the master’s thesis:

**Comparative Analysis of Static, Heuristic, and ML-Driven Task Scheduling in Medallion Lakehouse ETL Pipelines**

The project compares three Apache Airflow task-prioritisation strategies:

- Static priority scheduling
- HEFT-based heuristic priority scheduling
- XGBoost-driven priority scoring

The same Bronze, Silver, and Gold task structures are evaluated locally and on Azure Databricks.

## Repository structure

```text
airflow/
  dags/                  Airflow DAG definitions
  plugins/               HEFT and XGBoost scheduling helpers
  Dockerfile             Reproducible Airflow image
  docker-compose.yml     Local Airflow and PostgreSQL services

azure/
  notebooks/             Databricks Bronze, Silver, and Gold task notebooks
  configs/               Example experiment configuration

ml/
  features/              Feature extraction and training data
  training/              XGBoost training code and outputs

analysis/
  outputs/               Local benchmark tables and figures
  azure/
    bronze/              Initial Azure Bronze official results
    silver/              Initial Azure Silver official results
    gold/                Initial Azure Gold official results
    official_n5/         Final five-repetition Azure benchmark
    mechanisms/          Gold pairing and Bronze unlocking studies
    combined/            Final local/cloud combined analysis
    cost/                Final Azure cost analysis

docs/                    Experiment protocols and validation SQL
scripts/                 Local and Azure experiment runners
```

## Experiment inventory

### Local benchmark

- 10 repetitions per strategy
- 3 strategies
- 3 Medallion layers
- 90 layer-level runs
- 390 task-level observations

### Official Azure benchmark

- 5 repetitions per strategy and layer
- 45 layer-level runs
- 195 task-level observations

### Azure mechanism studies

Gold pairing:

- 3 opening pairs
- 5 repetitions per pair
- 15 runs
- 45 task-level observations

Bronze dependency unlocking:

- 2 controlled priority orders
- 5 repetitions per order
- 10 runs
- 60 task-level observations

The official benchmark and mechanism studies are analysed separately.

## Local Airflow

From PowerShell:

```powershell
cd "E:\Master thesis\medallion-etl-scheduling\airflow"
docker compose up -d postgres airflow-webserver airflow-scheduler
```

Airflow UI:

```text
http://localhost:8080
```

Stop services without deleting the PostgreSQL volume:

```powershell
docker compose down
```

Never use `docker compose down -v` unless the metadata volume is intentionally being deleted.

## Azure Databricks

Airflow submits the existing notebooks in `azure/notebooks/` to the configured Azure Databricks cluster. Secrets and personal access tokens must remain in local Airflow configuration and must never be committed.

## Reproducibility notes

- Raw TPC-DS data, runtime logs, virtual environments, credentials, and local Delta artifacts are excluded from Git.
- Final analysis CSV files are tracked.
- Generated figures and LaTeX result tables are stored under `analysis/outputs/`.
- Azure mechanism studies are not merged into the official Static–HEFT–XGBoost benchmark.
