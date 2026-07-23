# Azure Experiment Protocol

## Thesis Context

This protocol defines the Azure Databricks validation phase for the thesis:

**Comparative Analysis of Static, Heuristic, and ML-Driven Task Scheduling in Medallion Lakehouse ETL Pipelines**

The local benchmark is already completed and remains the main controlled statistical experiment. The Azure phase is used as a cost-aware cloud validation to check whether the local scheduling patterns remain meaningful under real Spark/Delta execution and to improve the cost discussion.

## Budget Constraint

Remaining primary Azure student credits: approximately **97 USD**.

The experiment must be completed without exceeding available credits.

### Budget Rules

| Rule | Limit |
|---|---:|
| Safe target spend | 60--70 USD |
| Warning threshold | 70 USD |
| Stop threshold | 80 USD |
| Emergency buffer | 17--27 USD |
| Maximum acceptable spend | 97 USD |

If the estimated spend reaches **80 USD**, experimentation stops unless an explicitly permitted alternative Azure account is used.

## Resource Principles

Only create the minimum resources needed for the experiment:

1. One Azure Resource Group
2. One Azure Storage Account with ADLS Gen2 enabled
3. One Azure Databricks Workspace
4. One small Databricks job cluster
5. A small dataset subset
6. Databricks notebooks/jobs for Bronze, Silver, and Gold

No extra services should be created unless required.

## Compute Rules

Use controlled Databricks compute:

| Setting | Choice |
|---|---|
| Cluster type | Job cluster preferred |
| Workers | 1 worker |
| Autoscaling | Disabled |
| Photon | Disabled initially |
| GPU | Not allowed |
| Auto-termination | 10 minutes |
| Runtime | Latest stable LTS Databricks Runtime |
| Dataset scale | SF1 or smaller subset |

The cluster must be terminated after every experiment session.

## Experimental Scope

The Azure experiment will validate the same scheduling strategies:

1. Static scheduling
2. HEFT-based priority scheduling
3. XGBoost-driven priority scheduling

The same Medallion structure will be used:

| Layer | Purpose | Example operations |
|---|---|---|
| Bronze | Raw ingestion | Read raw files and write Bronze Delta tables |
| Silver | Cleaning/transformation | Clean, filter, join, and write Silver Delta tables |
| Gold | Aggregation | Create analytical aggregates and write Gold Delta tables |

## Execution Plan

### Stage 0: Cost Safety Setup

Before running compute:

1. Check remaining Azure credits.
2. Create a budget alert around 70 USD.
3. Create a second alert around 85 USD if possible.
4. Confirm no old Azure resources are running.
5. Create one resource group for the thesis.
6. Tag resources with `project=master-thesis-scheduling`.

### Stage 1: Smoke Test

Run only:

- Static Bronze
- 1 repetition

Purpose:

- Verify Databricks workspace works.
- Verify storage access works.
- Verify Delta write works.
- Verify runtime logging works.
- Estimate rough cost per small run.

Stop if this smoke test appears unexpectedly expensive.

### Stage 2: Full Static Pilot

Run:

- Static Bronze
- Static Silver
- Static Gold
- 1 complete repetition

Purpose:

- Measure one full cloud pipeline runtime.
- Check whether cluster startup dominates runtime.
- Estimate cost per full pipeline repetition.

After this stage, decide whether the experiment can safely run 3 or 5 repetitions.

### Stage 3: Minimum Cloud Validation Benchmark

Run:

| Strategy | Repetitions |
|---|---:|
| Static | 3 |
| HEFT | 3 |
| XGBoost | 3 |

Expected task-level observations:

```text
3 strategies × 3 repetitions × 13 benchmarked tasks = 117 cloud task-level observations
```

This is the minimum acceptable Azure validation.

### Stage 4: Optional Extension

Only if cost is still clearly safe:

| Strategy | Repetitions |
|---|---:|
| Static | 5 |
| HEFT | 5 |
| XGBoost | 5 |

Expected task-level observations:

```text
3 strategies × 5 repetitions × 13 benchmarked tasks = 195 cloud task-level observations
```

Do not run 10 repetitions in Azure unless cost is proven to be extremely low.

## Metrics to Collect

For each task and layer:

| Metric | Description |
|---|---|
| strategy | static, heft, or xgboost |
| layer | Bronze, Silver, or Gold |
| run_id | unique cloud repetition identifier |
| task_id | benchmarked task name |
| start_time | task start timestamp |
| end_time | task end timestamp |
| duration_s | task runtime in seconds |
| layer_runtime_s | earliest task start to latest task end per layer |
| records_in | approximate input row count |
| records_out | approximate output row count |
| data_size_mb | approximate data size, if available |
| cluster_id/job_id | Databricks execution identifier |
| estimated_cost_usd | approximate cost estimate, if available |

## Analysis Plan

The Azure analysis will produce:

1. Azure runtime summary by strategy and layer.
2. Azure runtime improvement table.
3. Azure cost estimate by strategy and layer.
4. Local-vs-Azure comparison table.
5. Updated discussion of whether local patterns were confirmed.

If only 3 repetitions are run, the Azure result will be framed as **cloud validation**, not as a full statistical proof.

## Thesis Updates After Azure

After the Azure experiment, update:

| Thesis chapter | Required update |
|---|---|
| Chapter 3 Methodology | Add Azure Databricks validation setup |
| Chapter 4 Results | Add Azure validation results |
| Chapter 5 Discussion | Compare local and Azure behaviour |
| Chapter 6 Conclusion | Update final answers and future work |
| Appendix | Add protocol, resource configuration, and additional outputs |

## Stop Conditions

Stop Azure experimentation immediately if:

1. Spend approaches 80 USD on the primary account.
2. A cluster remains running unexpectedly.
3. The smoke test costs more than expected.
4. Databricks setup becomes unstable or repeatedly fails.
5. The dataset scale causes long runtimes.
6. Azure billing information is unclear.

## Final Positioning

The local benchmark remains the main controlled experiment:

```text
10 repetitions per strategy
390 task-level observations
Wilcoxon statistical testing
```

The Azure phase is a practical validation:

```text
smaller number of repetitions
real Spark/Delta execution
cloud runtime and cost estimation
```

This distinction should be kept clear in the final thesis.
