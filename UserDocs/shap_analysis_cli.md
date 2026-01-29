SHAP Analysis CLI
=================

This guide shows how to run SHAP analysis using a config file (YAML) and the
`shap` alias. It is a practical, user-facing walkthrough.

Quick start
-----------
1) Start from your config file (YAML).
2) Update the incident template and input bucket path if needed.
3) Run:
```
shap --config path_to_shap_config
```

What the command produces
-------------------------
The run writes an output directory and JSON artifacts. Typical outputs:
- `summary_json`: run metadata and evaluation summary
- `shap_json`: per-bucket SHAP contributions
- `output_root`: where additional artifacts are written

Example config (annotated)
--------------------------
```
incident_template: '59'
bucket_path: 'output/buckets/buckets.parquet'

output_root: 'output/shap_out'

# Window settings
lead_minutes: 3
max_minutes: 7

# Threshold policy (auto)
threshold_policy: negative-quantile

# Analysis knobs
window_analysis: true
episode_collapse: true
episode_gap_min_intervals: 5

# Time-series cross-validation (expanding window)
cv_strategy: time-series
cv_folds: 5
cv_test_size: null
cv_min_train_size: null

exclude_metrics:
  - info_count
  - error_count
  - warning_count

# Explicit output paths (readable JSON outputs)
summary_json: 'output/stuck540_substring_summary.json'
shap_json: 'output/stuck540_substring_shap.json'
shap_report_top_n: 5
details: compact
```

Field-by-field guidance
-----------------------

`incident_template` or `incident_substring` (choose one)
- Use `incident_template` when you already know the exact template ID.
- Use `incident_substring` when you only know part of the message text.
- The choice affects whether you see an interactive selection prompt.
- With `incident_template`, you can add `equivalent_templates` to treat multiple
  template IDs as the same incident family.

Example config (template ID):
```
incident_template: "59"
equivalent_templates: ["854", "535"]
bucket_path: "output/buckets/buckets.parquet"
```

Multi-line list format also works:
```
incident_template: "59"
equivalent_templates:
  - "854"
  - "535"
bucket_path: "output/buckets/buckets.parquet"
```

Example config (substring):
```
incident_substring: "CheckRebuildSpaceForNotifications"
bucket_path: "output/buckets/buckets.parquet"
```

Interactive prompt (substring only)
----------------------------------

Example (multiple matches, select all):
```
Matched 2 templates for incident_substring='are stuck':
   1. 77: All worker threads for <*> are stuck. Clearing queue as BUSY...
   2. 540: Console: <*> <*> Gateway : All worker threads for <*> are stuck. Clearing queue as BUSY...
Select templates by number (comma-separated) or 'all' [1]: all
Proceed with selected templates: 77, 540? [y/N]: y
```

Example (multiple matches, select specific IDs):
```
Matched 2 templates for incident_substring='are stuck':
   1. 77: All worker threads for <*> are stuck. Clearing queue as BUSY...
   2. 540: Console: <*> <*> Gateway : All worker threads for <*> are stuck. Clearing queue as BUSY...
Select templates by number (comma-separated) or 'all' [1]: 1,2
Proceed with selected templates: 77, 540? [y/N]: y
```

What it means:
- The CLI prints all matching templates and their IDs.
- The number after the dot is the list index; the second number on each line is the template ID.
- Selecting multiple templates treats them the same way as `equivalent_templates`.
- The resolved template IDs are used for the rest of the run.

`bucket_path` (required - must set)
- Path to the `buckets.parquet` file for this analysis.

`output_root` (OK to change)
- Directory to write artifacts for this run.
- Change it to avoid overwriting previous runs.

`lead_minutes` (OK to change)
- Controls how far back the window starts from the incident time. This is effectivly how long proceeding an incident can a cause "get credit". 
- This is the primary setting you should be playing with atm. Im working on a soloution to dynamicly calculate the best values but for now it a bit of guess and check.

`max_minutes` (avoid changing)
- Maximum lookback in minutes for auto-expansion if insufficient data sound in lead minute window.
- Use the same value as `lead_minutes` to disable expansion.
- Should bassically be Irrelevant 99.99% of the time but acts as a failsafe. 

`threshold_policy` (avoid changing)
- `negative-quantile` selects a threshold based on negative examples.
- `fixed` uses the provided `threshold` value.
- Adjusts model bias toward negative buckets since they are much more common you should almost always leave this on negative-quantile. 

`window_analysis` (OK to change)
- Enables window-level clustering and association analysis.
- Keep `true` if you want window insights.
- Adds some extra details to the anylasis report for the cost of some extra time. Shap anylais is already pretty fast so it doesnt cost much but if you dont care about it feel free to disable to save some time. 

`episode_collapse` / `episode_gap_min_intervals` (avoid changing)
- Causes repeat incidents to be collapsed into single episodes.
- `episode_gap_min_intervals` is the minimum number of *inter-arrival intervals* required before the system will *learn* an episode gap automatically. If there are fewer intervals than this, it skips learning and keeps the default behavior.
- Ive found its gennerally a good idea to leave this on. If you change this you should have a good reason for doing so. 

`cv_strategy` (avoid changing)
- `time-series` enables time-based cross-validation. This if gennerally just better for log anylasis. 
- `none` skips CV and uses a single split.

`cv_folds`, `cv_test_size`, `cv_min_train_size` (avoid changing)
- Tuning controls for time-series CV.
- Leave as-is unless you need tighter validation windows.

`exclude_file` (OK to change)
- YAML file listing templates or regex patterns to exclude.
- Use this to remove symptom templates or known confounders.

`exclude_metrics` (OK to change)
- Remove specific metric-derived features (like `info_count`).
- Helpful to avoid leakage or reduce feature noise.

`summary_json` (OK to change)
- JSON summary output path (easy to inspect/run comparisons).
- Change if you want the summary stored elsewhere

`shap_json` (OK to change)
- SHAP output path for per-bucket contributions.
- Change if you want the summary stored elsewhere

`shap_report_top_n` (OK to change)
- Number of features/templates included in the aggregate SHAP summary tables.
- Change if you want more or less top results

`details` (OK to change)
- `compact` (default) shows only the aggregate tables (top templates by abs, top features by abs).
- `full` shows the full summary (aggregate tables + per-bucket tables + paths).

Common tweaks
-------------
- **Point to new data:** update `bucket_path`.
- **Change output location:** update `output_root`, `summary_json`, and `shap_json`.
- **Keep the window fixed:** tweak `lead_minutes` value.
- **Reduce feature set:** add items to `exclude_metrics` or provide `exclude_file`.

Example output explained
------------------------

Example output (truncated):
```
Matched 1 templates for incident_substring='CheckRebuildSpaceForNotifications':
   1. 2: CheckRebuildSpaceForNotifications started, with stress level: <*> Is parallel: <*>
Use this template family? [y/N]: Resolved incident family: incident_template=2, equivalent_templates=[]
GBDT Evaluation ----------------------------
Windows processed: 16 positives, 9 negatives
Model backend: LightGBM (linear tree)
Feature count: 36
Train AUC: 1.000
Confusion matrix @ threshold 1.00: TP=2, FP=1, TN=1, FN=0
Accuracy: 0.750  Precision: 0.667  Recall: 1.000  FPR: 0.500
Notes: prob=surrogate model predicted probability of the positive class; FPR=false positive rate=FP/(FP+TN).
SHAP note: contributions are in log-odds space; positive values push prob up.

SHAP overview (aggregated across buckets):
                        Top templates by abs(mean SHAP)                         
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ mean(SHAP) ┃ mean(|SHAP|) ┃ template_id ┃ template_str                       ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│     +0.123 │        6.889 │ 58          │ Vol-3223-1-Client1 <*> spare       │
│     +0.001 │        0.562 │ 59          │ All worker threads for             │
└────────────┴──────────────┴─────────────┴────────────────────────────────────┘
                         Top features by abs(mean SHAP)                         
┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┓
┃ mean(SHAP) ┃ mean(|SHAP|) ┃ frac+ ┃ feat ┃ template_id ┃ var_type ┃ template ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━┩
│     +0.134 │        5.227 │  0.64 │ temp │ 58          │          │ Vol-3223 │
└────────────┴──────────────┴───────┴──────┴─────────────┴──────────┴──────────┘
Full aggregate report: tmp_userdocs/out_shap_substring/incident-2/.../shap_report_aggregate.json
Per-bucket report: tmp_userdocs/out_shap_substring/incident-2/.../shap_report_per_bucket.txt
                  Bucket b_20260123_1750 (label=1, prob=1.000)                  
┏━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━┓
┃   SHAP ┃ feature             ┃ template_id ┃ template_str  ┃ var  ┃ var_type ┃
┡━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━┇━━━━━━━━━━┩
│ +4.183 │ template_count_58   │ 58          │ Vol-3223-1-Cl │      │          │
│ +1.016 │ metric_delta_total_ │             │               │      │          │
│ +0.668 │ template_58_var1=st │ 58          │ Vol-3223-1-Cl │ var1 │ other    │
└────────┴─────────────────────┴─────────────┴───────────────┴──────┴──────────┘
```

What each section means
-----------------------

Incident selection (substring flow)
- Shows all templates matching your substring.
- You confirm the selection; the chosen IDs become the incident family.

GBDT Evaluation
- `Windows processed`: how many positive/negative windows were created.
- `Feature count`: number of features used by the surrogate model.
- `Train AUC`: training AUC (not a test metric).
- `Confusion matrix @ threshold`: TP/FP/TN/FN using the selected threshold.
- `Accuracy/Precision/Recall/FPR`: standard metrics for the model outputs.

SHAP overview (aggregated across buckets)
- **Top templates by abs(mean SHAP)**: template IDs ranked by average impact magnitude.
- **Important:** Rows are sorted by `mean(|SHAP|)` (average absolute impact). This is the
  most reliable single importance signal because it captures strength regardless of sign.
  `mean(SHAP)` can be misleading when a feature flips sign across buckets (common for
  lead-minutes features that are often negative).
  - `mean(SHAP)`: average signed contribution (positive pushes probability up).
  - `mean(|SHAP|)`: average absolute contribution (magnitude only).
- **Top features by abs(mean SHAP)**: individual features ranked by average impact.
  - `feature` is the exact feature name (metric or template feature).
  - `frac+` is the fraction of buckets where the SHAP value is positive.
  - `template_id`, `var_type`, `template_str` appear when the feature is template-based.

Section overview (how to read the SHAP output)
----------------------------------------------

Top templates by abs(mean SHAP)
- This is an aggregate view of template impact.
- All features tied to the same template are combined to produce the template-level score.
- Use this section to understand which templates matter most overall.

Top features by abs(mean SHAP)
- This is a feature-level view (metrics + template features).
- It includes metrics (e.g., `metric_delta_total_events`) and template sub-features
  such as variable-specific features (e.g., `template_58_var1=starting`).
- Use this section to see which specific signals are driving the model.

Per-bucket tables
- This section shows a small set of positive buckets and their top SHAP contributors.
- It is meant for “why this bucket scored high?” analysis.

Where to find full outputs
- The summary JSON includes the full per-bucket summary table.
- The SHAP JSON includes every feature contribution for every bucket.

Field-by-field breakdown (from the example tables)
--------------------------------------------------

Top templates by abs(mean SHAP)
- `mean(SHAP)`: signed mean contribution for this template across buckets.
- `mean(|SHAP|)`: mean absolute contribution (primary importance signal).
- `template_id`: template identifier.
- `template_str`:  template text (truncated for display).

Top features by abs(mean SHAP)
- `mean(SHAP)`: signed mean contribution for this feature.
- `mean(|SHAP|)`: mean absolute contribution (primary importance signal).
- `frac+`: fraction of buckets where this feature’s SHAP is positive.
- `feature`: the exact feature name (e.g., `template_count_58`, `metric_delta_total_events`,
  `template_58_var1=starting`).
- `template_id`: template ID if the feature is template-derived, else blank.
- `var_type`: coarse type for template-variable features (e.g., `numeric`, `other`).
- `template_str`: template text when the feature ties to a template.

Per-bucket tables
- Each table is a single bucket (window).
- `prob` is the model’s predicted probability for that bucket.
- Rows show top contributing features, with:
  - `SHAP` (signed contribution in log-odds)
  - `feature` name
  - `template_id`/`template_str` for template features
  - `var`/`var_type` for template variable features
