# Comparative Analysis of Static, Heuristic, and ML-Driven Task Scheduling in Medallion Lakehouse ETL Pipelines

**Research implementation and historical experiment archive** | RWTH Aachen University | M.Sc. Data Analytics and Decision Science

**Author:** Gurusaran Sivakumar  
**Thesis title:** *Comparative Analysis of Static, Heuristic, and ML-Driven Task Scheduling in Medallion Lakehouse ETL Pipelines*  
**Thesis cover date:** 27 October 2026  
**Chair:** Chair of Data and Business Analytics  
**Study platform:** Apache Airflow, Python, Azure Databricks, Spark, Delta Lake and PostgreSQL

> [!IMPORTANT]
> **Archive status: partially reproducible, not a complete replay of the submitted thesis.** This repository includes the local benchmark, the **earlier** 45-run Azure benchmark, and three controlled cloud studies. The submitted thesis uses a **different, repeated** 45-run Azure benchmark as its primary cloud comparison. The repeated benchmark's raw records, corresponding executed-source revision and full figure-generation pipeline are **not supplied in this ZIP**. Consequently, successful offline tests of this archive do **not** independently reproduce all results in the submitted thesis. Read [the alignment audit](docs/SUBMISSION_ALIGNMENT_AUDIT.md) before reusing or publishing claims from this repository.

## Quick start: verify the archived evidence, without cloud access

From the repository root, with Python 3.12 recommended:

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt -r requirements-analysis.txt
python analysis/verify_archived_results.py
python -m unittest discover -s tests -v
```

Expected from the archive: five phase checks totaling **175 run summaries / 735 task observations**, and **six passing tests**. Note that those 175 run summaries refer to this archive's historical datasets, not independently to the repeated-Azure set used in the thesis.

For the final local Mann–Whitney/Holm analysis, independently generated from the archived local data:

```bash
python analysis/statistical_tests_independent.py --input analysis/outputs/run_layer_metrics.csv --output local_mannwhitney_recomputed.csv
```

Compare the generated CSV with `analysis/outputs/local_runtime_mannwhitney_holm.csv`. Do not overwrite archived evidence. More commands, expected results, platform-specific guidance and limitations appear in **[REPRODUCE.md](REPRODUCE.md)**.

## What the study evaluates

Apache Airflow selects among ready tasks when the number of eligible tasks exceeds available execution capacity. The study compares three **Airflow-level priority policies** for Bronze, Silver and Gold Medallion ETL layers:

| Policy | Actual approach | Important scope boundary |
|---|---|---|
| Static | Fixed priorities, with locally default versus manually weighted cloud baselines | Cloud and local Static configurations are not numerically identical |
| HEFT-based | Upward-rank heuristic mapped to Airflow `priority_weight` | Not full heterogeneous HEFT processor assignment |
| XGBoost-labelled | Offline model training on an engineered priority target and historical fixed policy tables | Historical cloud weights in this archive reflect the engineered target, **not the saved model's exact predictions**; no online adaptation |

The unit of analysis is a **completed layer run**, and the primary outcome is the **layer processing window**: the last computational task's end time minus the first computational task's start time. For the submitted thesis's repeated cloud comparison, the reference timing source is the Databricks **remote-job** start/end records. Historical metrics in this ZIP have other timing provenance. These measurements are not equivalent to total Airflow DAG duration, summed task-seconds, full-pipeline makespan, or CPU usage.

### Workload and layer structure

Six selected tables in `samples.tpcds_sf1` are processed: `date_dim`, `item`, `customer`, `web_sales`, `store_sales`, and `catalog_sales`. The selected subset is approximately **5,229,704 rows** and **273.93 MB** of reported compressed source data. This is **not** an execution of the complete TPC-DS benchmark.

- **Bronze:** six ingestion tasks; `date_dim` and `item` release sales ingestions, while `customer` is also required by store sales.
- **Silver:** `clean_sales`, `clean_customer`, and `clean_item` feed the final `join_fact_sales` barrier.
- **Gold:** `agg_customer_ltv`, `agg_sales_by_region`, and `agg_top_items` are independent at the DAG level and can overlap.

The policy comparison fixes `max_active_tasks=2`: each layer initially has three eligible computational tasks, creating a genuine admission choice. A **separate Gold mechanism experiment** compares concurrency limits of 1, 2 and 3. Two is an experimental constraint, not a proven optimal cluster size.

## Inventory: what is and is not in this ZIP

| Component | Included | Notes |
|---|---|---|
| Airflow DAGs and local scheduler helpers | Yes | `airflow/dags/`, `airflow/plugins/` |
| Parameterised Azure Bronze, Silver, Gold notebook sources | Yes | `azure/notebooks/03_bronze_task.py`, `05_silver_task.py`, `06_gold_task.py` |
| Earlier Bronze smoke-test notebook | Yes | `azure/notebooks/01_bronze_static.py`, not the main benchmark notebook |
| XGBoost feature extraction and training scripts | Yes | `ml/features/`, `ml/training/` |
| Saved XGBoost model and verification output | Yes, **in the ZIP** | `ml/training/outputs/xgboost_priority_model.joblib`; note original-version warnings possible |
| Archived local benchmark and final local inferential table | Yes | `analysis/outputs/` |
| Earlier Azure benchmark exports | Yes | `analysis/azure/official_n5/` and layer-specific exports |
| Bronze-unlocking, Gold-pairing, Gold-concurrency exports | Yes | `analysis/azure/mechanisms/` |
| Thesis's **repeated** Azure benchmark raw data | **No** | Thesis uses different means and remote-job timing records |
| Exact source revision used in repeated Azure execution | **Not authenticated** | Historical DAG factories cannot establish it |
| One-command recreation of all final thesis figures | **No** | Some historical plots and statistical scripts are present |
| Original complete native LaTeX thesis source | **No** | Maintained separately, if available; not contained in this implementation archive |
| Cloud workspace, credentials, cluster | **No** | New Azure/Databricks environment required |

### Experimental counts represented in the archived CSVs

| Phase | Runs | Task rows | Archive location |
|---|---:|---:|---|
| Local benchmark | 90 | 390 | `analysis/outputs/run_layer_metrics.csv` (run summaries; task rows accounted for in counts) |
| **Earlier** Azure benchmark | 45 | 195 | `analysis/azure/official_n5/` |
| Bronze dependency unlocking | 10 | 60 | `analysis/azure/mechanisms/bronze_unlocking/` |
| Gold opening pairs | 15 | 45 | `analysis/azure/mechanisms/gold_pairing/` |
| Gold concurrency | 15 | 45 | `analysis/azure/mechanisms/gold_concurrency/` |
| **Archive total** | **175** | **735** | Verified by the offline checker |

**Important:** The submitted thesis also reports 175 main-study runs, but its 45 cloud benchmark runs are the **repeated model-derived** phase, not this archive's earlier 45. The matching total is therefore **not proof that the same experiment set is archived**.

## Results: historical archive versus submitted thesis

The numerical tables must not be mixed.

**Earlier cloud benchmark preserved in this repository** (seconds, five runs per cell):

| Layer | Static | HEFT-based | XGBoost-labelled |
|---|---:|---:|---:|
| Bronze | 128.545 | 141.546 | 118.035 |
| Silver | 117.797 | 122.541 | 123.276 |
| Gold | 93.902 | 90.614 | 96.323 |

**Submitted-thesis repeated cloud benchmark**, *for reference only; raw evidence is not included here* (seconds, five runs per cell):

| Layer | Static | HEFT-based | XGBoost-derived |
|---|---:|---:|---:|
| Bronze | 226.697 | 246.057 | 187.639 |
| Silver | 165.323 | 152.387 | 154.377 |
| Gold | 399.317 | 353.782 | 346.976 |

The historical archive's `official_n5` filenames predate the repeated benchmark and **must not be mistaken for the thesis's principal Azure results**. The old and new measurements also have non-identical timing provenance. Do not pool them, compute a pooled average, or claim a direct causal comparison across sessions.

The controlled mechanism results in the archive are: Bronze early versus delayed unlocking **123.190 versus 153.199 s**; Gold pairing means **90.313, 94.032 and 107.586 s**; Gold concurrency-1/2/3 processing-window means **122.061, 77.785 and 56.144 s**. These are separate exploratory experiments, not extra strategy-benchmark repetitions. The pair experiment's second-wave task is not an isolated cold-run baseline.

## Model/priority provenance

The XGBoost model was trained offline on a target explicitly constructed from task features. The training data include **130 rows across 13 distinct task identities**, with repeated identities in a row-wise train/test split. High target-prediction accuracy does not prove that the model learned to minimise observed makespan or generalises to unseen DAGs.

The original, historically fixed Airflow priorities are in `airflow/plugins/xgb_scheduler.py` and the Azure common DAG factories. They follow the engineered target; they are **not numerically identical** to scores from `ml/training/outputs/xgboost_priority_model.joblib`. The supplied `ml/verification/outputs/XGBoost_Offline_Rank_Verification.csv` shows identical rank order within each layer, but **7 of 13 rounded task weights differ** from saved-model predictions. See the exact checks in [REPRODUCE.md](REPRODUCE.md).

Do **not** silently replace historical weights with predictions or claim that the archived first-phase runs used the exact predicted weights. Likewise, the first-phase archive does not establish which precise source revision produced the separately reported repeated benchmark.

## Repository map

```text
README.md                      Overview and caveats
REPRODUCE.md                   Verified offline procedure; cloud setup limits
PROJECT_REPAIR_CHANGELOG_2026-10-19.md   Historical repairs
AUDIT_HANDOVER_2026-10-28.md    This documentation-only review
docs/SUBMISSION_ALIGNMENT_AUDIT.md     Thesis/archive discrepancies and resolution checklist
airflow/
  Dockerfile, docker-compose.yml, .env.example
  dags/                     Local and historical Azure DAGs
  plugins/                  HEFT and historical fixed XGBoost policies
azure/
  notebooks/                01 smoke test; 03 Bronze, 05 Silver, 06 Gold parameterised notebooks
  configs/                  Historical example configuration
ml/
  features/                 Feature extraction and source training CSVs
  training/                 Training code, outputs, saved model
  verification/             Model versus historical Airflow priority comparison
analysis/
  outputs/                  Archived local run summaries and older plots
  azure/official_n5/        Historical/earlier Azure 45-run exports
  azure/mechanisms/         Three controlled cloud studies
  statistical_tests_independent.py    Final local inferential analysis
  verify_archived_results.py        Offline archive integrity checks
scripts/                      Historical PowerShell runners; contain machine-specific paths
tests/                        Six offline unit/integrity tests
```

## Reproducing an Airflow or Azure experiment

Offline archived results are the reproducible part of this ZIP. For new runs, first read [REPRODUCE.md](REPRODUCE.md) and [the Azure setup guide](docs/azure_airflow_setup.md).

**High-level prerequisites:** Docker and Docker Compose; PowerShell for the historical scripts; Python 3.12 recommended; a new Azure Databricks workspace with permission to use the six SF1 sample tables, a suitable single-node compute resource, Unity Catalog, and access to create tables; locally generated Airflow/PostgreSQL secrets; notebook imports and a Databricks Airflow connection.

The Compose file lives in `airflow/`. From the repository root, copy `airflow/.env.example` to `airflow/.env`, populate its three secret fields with independent random values, then run Compose **from `airflow/`**. The web and database ports bind to localhost. The historical PowerShell scripts contain `E:\Master thesis\...` paths and are **not portable unchanged**. Before running a fresh experiment, adapt and inspect all paths, run IDs, cloud resource identifiers, output namespaces and expected inputs. Pilot and historical scripts should not be used to claim exact reproduction of the repeated thesis benchmark.

## Reproducibility, integrity, and security boundaries

- **Verified offline here:** 50 Python files parse, six tests pass, and run/task-count and timing checks pass for the datasets actually archived. Independent local Mann–Whitney/Holm output matches the supplied archived table.
- **Not tested here:** Docker build or boot, live Airflow DAG imports, PowerShell execution, Databricks permissions, notebook imports, remote Spark execution, regenerated cloud measurements, and full thesis-figure regeneration. Do not interpret offline checks as integration testing.
- **Secrets:** Do not upload `.env`, PATs, API keys, connection exports or container logs. The ZIP has a template `.env.example`, not an active credential file. Prior Git history may require independent secret review and rotation.
- **Versioning:** The Python requirements give compatible versions rather than a frozen original environment. Docker image and package digests are not pinned to historical immutable builds.
- **Scientific integrity:** Results and model artifacts are archived as supplied, not silently regenerated. New executions are new experimental samples and should be dated and reported separately.

## Citation

```bibtex
@mastersthesis{sivakumar2026medallion,
  author = {Gurusaran Sivakumar},
  title  = {Comparative Analysis of Static, Heuristic, and ML-Driven Task Scheduling in Medallion Lakehouse ETL Pipelines},
  school = {RWTH Aachen University},
  year   = {2026},
  type   = {Master's Thesis}
}
```

No external open-source licence is asserted by this archive. Confirm ownership and institutional requirements before public redistribution.