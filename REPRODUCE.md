# Reproducibility guide

This runbook is for the **supplied implementation/archive ZIP**, not for an exact replay of every result in the submitted thesis. Start with [README.md](README.md) and [the submission-alignment audit](docs/SUBMISSION_ALIGNMENT_AUDIT.md). The archived Azure files are the **earlier** benchmark, whereas the thesis's primary Azure results come from a **separate repeated** benchmark. No source file or numerical evidence has been retroactively altered to hide this difference.

## Reproduction levels

| Level | Requires | Verifiable with supplied ZIP? |
|---|---|---|
| A: archive counts, timestamp and task-ID consistency | Python standard library | **Yes, tested** |
| B: local Mann–Whitney/Holm results | pandas, SciPy | **Yes, tested** |
| C: XGBoost historical weights versus saved-model predictions | trusted supplied joblib model, ML dependencies | **Yes, tested; compatibility warning** |
| D: rerun local Airflow tasks | Docker, PostgreSQL, PowerShell, configured DAGs | **Not tested here** |
| E: rerun archived earlier Azure benchmark | D plus new Azure/Databricks environment and compatible data | **Not tested here** |
| F: exact repeated Azure benchmark reported in thesis | Repeated run records, executed-source revision, platform/timing provenance | **Not reproducible from supplied ZIP** |
| G: regenerate every final thesis figure and table | Complete plotting/export pipeline and primary repeated Azure source data | **Not reproducible from supplied ZIP** |

## 1. Work in an isolated environment

Unzip the repository and open a terminal **at the `medallion-etl-scheduling/` root**. Python 3.12 is recommended to approximate the original Airflow image environment (not a verified historical lock).

Linux or macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt -r requirements-analysis.txt
```

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt -r requirements-analysis.txt
```

No real cloud credentials are required for Levels A–C. The requirements are **minimum compatible constraints**, not a frozen lockfile of the experiment environment. Do not overwrite the supplied archived outputs.

## 2. Run archive integrity checks (Level A)

```bash
python analysis/verify_archived_results.py
```

**Verified output:** local 90/390; archived earlier Azure 45/195; Bronze unlocking 10/60; Gold pairing 15/45; Gold concurrency 15/45; total **175 run summaries and 735 task observations**.

The checker tests run IDs, task counts, duplicate and orphan tasks, timestamps and processing-window consistency for the exported sources. It does **not** prove that the cloud runs occurred as claimed, verify provider-side remote job history or establish that the exported cloud data correspond to the thesis's *repeated* benchmark.

## 3. Run the offline tests (Level A/B)

```bash
python -m unittest discover -s tests -v
```

**Verified here:** six tests pass. These test HEFT rank calculations, cycle checks, historical fixed XGBoost ordering, archive consistency and the local statistical output. No live Airflow, Docker, Spark or Databricks integration is exercised.

## 4. Regenerate final *local* statistical comparison (Level B)

```bash
python analysis/statistical_tests_independent.py --input analysis/outputs/run_layer_metrics.csv --output local_mannwhitney_recomputed.csv
```

Compare `local_mannwhitney_recomputed.csv` with `analysis/outputs/local_runtime_mannwhitney_holm.csv`. The six predefined independent-sample exact Mann–Whitney contrasts use Holm adjustment; three comparisons have adjusted p < 0.05. This is **local** analysis, not proof of a causal advantage in cloud execution.

The files `analysis/statistical_tests.py`, `analysis/outputs/wilcoxon_results.csv`, `analysis/outputs/table_wilcoxon_runtime.*`, and `analysis/generate_figures.py` are **historical/superseded for inferential purposes**. Running `generate_figures.py` can overwrite existing historical outputs. Do not substitute them for the final thesis figures and statistical results.

## 5. Verify historical XGBoost provenance (Level C)

Only unpickle a model artifact you trust; `joblib.load` can execute arbitrary code. This project contains a saved model from the provided ZIP. In a trusted isolated environment:

```bash
python ml/verification/verify_xgb_priority_ranking.py --project-root . --out xgb_rank_check_recomputed.csv
```

**Verified here:** the 13 saved-model predictions produce the same ordering as the historical fixed table within all three layers. But the saved predictions are not numerically identical to that table, and **7 of the 13 rounded saved-model predictions differ from the historical Azure integer weights**. Package-version warnings can occur when loading the saved model with more recent scikit-learn/XGBoost releases.

**Do not retrain as an initial verification step.** `python ml/training/train_xgboost.py` writes over the model, metrics, predictions and feature-importance outputs. For retraining, first make a separate working copy, retain hashes of the archived inputs, and treat the outputs as a **new model** rather than the historical deployed model. The training target is engineered, with repeated task identities; prediction accuracy is not optimal-scheduler evidence.

## 6. Optional local Airflow setup (Level D; not verified in this review)

Requirements: compatible Docker/Compose installation, sufficient RAM, no conflicting localhost ports 5432/8080, and PowerShell to use the historical runner scripts. The Compose environment is a **local development configuration**, not a secure production deployment.

At repository root:

```bash
cp airflow/.env.example airflow/.env
```

On Windows PowerShell use `Copy-Item airflow/.env.example airflow/.env`. Set unique, long random values for `AIRFLOW_POSTGRES_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`, and `AIRFLOW_WEBSERVER_SECRET_KEY`. Use URL-safe characters for the database password. `airflow/.env` is intentionally ignored by Git.

```bash
cd airflow
docker compose config
docker compose build
docker compose up -d postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list
```

Open http://127.0.0.1:8080 and sign in using the configured local admin password. Confirm tasks and database are healthy before triggering any run. **These commands were not executed in this audit runtime** because Docker is unavailable.

The historical `scripts/*.ps1` runners include absolute developer-machine paths such as `E:\Master thesis\medallion-etl-scheduling`. Before executing a historical runner on a new computer, edit those references to your repository path, review the run-ID prefixes, `StartRep`/`EndRep`, expected DAG IDs, and SQL queries, and run one smoke test before a batch. **A new run does not retroactively reproduce an original historical run.**

## 7. Optional Azure Databricks setup (Level E; not verified in this review)

This needs a new workspace because the historical workspace was deleted. Expect platform changes and costs. Use the cloud provider's current procedures and permissions.

1. Create or access an appropriate Databricks workspace with Unity Catalog, `samples.tpcds_sf1` read access and a compatible single-node cluster. For a historical comparison the thesis reports UAE North, Databricks Runtime 17.3 LTS, Spark 4.0, `Standard_D4ds_v4` (4 cores/16 GB), Photon off, 10-minute auto-termination. Equivalent current configurations may not produce comparable runtime measurements.
2. Import **three parameterised notebook source files** into Databricks as workspace notebooks: `azure/notebooks/03_bronze_task.py` to `/Shared/03_bronze_task`, `05_silver_task.py` to `/Shared/05_silver_task`, and `06_gold_task.py` to `/Shared/06_gold_task`. The `01_bronze_static.py` source is an earlier synthetic smoke test, not the main TPC-DS task implementation.
3. Create a writable target catalog and schema. The historical default names were `dbw_thesis_etl_scheduling` and `thesis_etl_scheduling`; **choose your own if you lack permissions**. Notebook code uses those values as parameters.
4. In Airflow, create a Databricks connection named `databricks_azure` using a valid workspace host and appropriately scoped credential. Do not put credentials in code or Git.
5. In Airflow Variables configure `databricks_azure_cluster_id`, `databricks_bronze_notebook_path`, `databricks_silver_notebook_path`, `databricks_gold_notebook_path`, `databricks_target_catalog`, and `databricks_target_schema`.
6. Check Airflow import errors and ensure the operator's notebook paths, task IDs, connection, permissions and output namespaces are valid. Trigger **one Static Bronze smoke test** and inspect Databricks job history, task parameters, six outputs and the Delta metrics table. Then prepare compatible Bronze inputs before testing Silver; prepare the Silver fact table before Gold. Ensure no other benchmark experiment is active.
7. For any subsequent historical benchmark runner, inspect its hardcoded path and intended run IDs and verify the Airflow capacity limit. Take an independent export of job start/end times and the exact applied priorities. Do not combine pilot, failed, earlier, repeated and controlled runs in analysis.

See [docs/azure_airflow_setup.md](docs/azure_airflow_setup.md) and the historical `docs/azure_experiment_protocol.md` for supplemental operational context. Those historical documents do not establish a verified end-to-end reproduction today.

## 8. What remains to recreate the *submitted thesis's* primary cloud result (Level F)

The submitted PDF's repeated Azure results require evidence not found in this ZIP:

- The 45 repeated strategy-by-layer **run summaries** and the 195 **original Databricks remote job** records with IDs/start/end times;
- The executed DAG and notebook source revision or documented change list for that repeated phase;
- The applied 13 saved-model-derived integer priorities, with supporting model/feature/version fingerprints;
- Run-order/session metadata, input/output table identifiers and the validated output evidence;
- Analysis code that reproduces the repeated-Azure means, sensitivity checks and final cloud figures from those records.

**Do not relabel the existing `analysis/azure/official_n5` exports as the repeated phase.** They numerically match the earlier results in Appendix B.1 of the submitted thesis, not Table 4.3 of its main results.

## 9. Integrity, security, and responsible interpretation

Archived CSVs, manifests, priority tables and models should be retained without modification. Use new output paths for recalculations, avoid committing credentials, and review any former published Git history separately for secrets. The checks performed here are numerical/source-level checks, not independent cloud-record authentication or a guarantee of thesis authorship, academic-integrity compliance or scientific correctness.

The submitted thesis PDF is the reference for its reported research claims; this repository is an implementation and historical evidence archive with the gaps described above.