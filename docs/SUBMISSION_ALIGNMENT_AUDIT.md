# Archive-to-thesis alignment audit

**Audit date:** 28 October 2026  
**Source:** User-supplied `medallion-etl-scheduling_REPAIRED_CLEAN_2026-10-19.zip` and the previously supplied 103-page thesis submission PDF.  
**Scope:** File inventory, README and runbook accuracy, offline tests, internal data integrity, priority provenance and gaps to reproduce the submitted thesis.

## Executive finding

**The source archive contains the expected core DAGs, parameterised notebooks, scripts, ML model and several historical exports. It is not, as supplied, a complete reproduction package for the final submitted thesis.** The most consequential issue is a phase mismatch: the archive's `official_n5` cloud results correspond to the thesis's *earlier* benchmark, while its Chapter 4 reports a different *repeated* benchmark as primary evidence.

## Evidence matrix

| Issue | Evidence in ZIP | Reference in submitted PDF | Consequence |
|---|---|---|---|
| Cloud benchmark phase mismatch | `analysis/azure/official_n5/azure_official_n5_descriptive_statistics.csv` Bronze Static 128.545; Gold Static 93.902 | Chapter 4 Table 4.3: Bronze Static 226.697; Gold Static 399.317. Appendix B.1 contains archive's earlier numbers | The ZIP does not independently regenerate the thesis's primary Azure result |
| XGBoost priority numeric provenance | `airflow/plugins/xgb_scheduler.py`, Azure common DAG factories and `ml/verification/outputs/XGBoost_Offline_Rank_Verification.csv`: target-derived historical weights, 7/13 rounded weights differing from saved predictions | Methodology Sections 3.6.2, 3.10 describe repeated benchmark as saved-model-derived | Historical archive is insufficient to authenticate executed repeated-phase policy; matching ranks do not prove same numeric implementation |
| Same run count hides data difference | `analysis/verify_archived_results.py` counts 90 local + 45 **earlier Azure** + 40 controlled = 175 | Thesis counts 90 local + 45 **repeated Azure** + 40 controlled = 175 | Passing the 175/735 check is not proof of thesis alignment |
| Historical PowerShell paths | Multiple files in `scripts/` have `E:\Master thesis\...` absolute working paths | Not a thesis data issue | Runners are not portable without manual editing/testing |
| Notebook inventory | Four `azure/notebooks/*.py`: one smoke test plus three parameterised task sources | Thesis has Bronze/Silver/Gold notebook workloads | Main task implementations exist, but not a separately authenticated repeated-phase revision |
| Figure regeneration | `analysis/generate_figures.py` invokes historical Wilcoxon outputs | Thesis local inference uses independent Mann–Whitney/Holm | Historical charting is not a one-click build of final thesis figures |
| Model tracked in ZIP versus Git | `*.joblib` in `.gitignore` originally ignored a saved model even though the ZIP includes it | Not a thesis data issue | Unless explicitly force-added/tracked, the model may be missing from a fresh GitHub push; fixed in audited `.gitignore` copy |
| Editorial metadata | Original README said 28 Oct 2026; thesis cover shows 27 Oct 2026. Original `REPRODUCE.md` said “84-main-page” | Thesis has Chapters 1–6 on printed pp. 1–82 and cover dated 27 Oct | Documentation clarified in audited copy, not an experimental contradiction |

## Verified operations in this audit runtime

1. ZIP listing: **134 files**, including four `.py` Databricks notebook sources, three parameterised main task notebooks, DAGs, two scheduler plugins, CSV exports, a `.joblib` model and validation SQL.
2. **50 Python files** parsed successfully with the local interpreter.
3. `python -m unittest discover -s tests -v`: **6 tests passed**.
4. `python analysis/verify_archived_results.py`: **5 phase checks passed**, total **175 run summaries / 735 task observations** for archive contents.
5. Recomputed local Mann–Whitney/Holm analysis matched the expected archived reference CSV using the repository tests.
6. Recomputed saved-model versus fixed-table rank check: **13/13 task orderings agree within layers; 7/13 rounded predictions differ from historical Azure integer weights**. Loading a model with newer ML package versions emitted compatibility warnings.
7. No live Docker/Compose, PowerShell, Airflow or Databricks integration tests could be performed in this runtime; the current environment lacks Docker and PowerShell.

## Required for exact thesis-level reproducibility

1. Obtain **original repeated** cloud run exports with 45 run summaries and 195 original remote Databricks job histories/timestamps, plus the associated applied priorities.
2. Locate the executed DAG/notebook code version or explicitly document uncertainty about its provenance. Do not make the historical code appear to be the authenticated later version.
3. Produce a separate read-only reproduction script for final Azure Table 4.3/4.4 and the associated sensitivity analyses using the recovered repeated-phase records.
4. Include the versions or build metadata actually used in the original measurements where available; do not invent a lockfile from today's environment.
5. Complete a clean Docker/Compose boot, DAG import, notebook smoke test and carefully documented cloud replication **on an independently provisioned machine** if a live reproduction claim is required.
6. Recover scripts/data to regenerate all final thesis figures and tables, or explicitly distinguish rendered submission figures from reproducible data outputs.

**Do not replace missing data with generated values, rename earlier data as later data or claim that the current 175/735 check verifies the repeated benchmark.** If original sources cannot be recovered, report this as a reproducibility limitation.

## Change policy of the audited package

The audited package updates **documentation and `.gitignore` only**. All original Python, notebook, PowerShell, CSV, model, statistical-result, figure, configuration and SQL evidence bytes are preserved unchanged. A separate file-hash list is generated for audit traceability. The edits do not create additional experimental evidence or resolve the missing repeated-phase results.