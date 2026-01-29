Bucket Select CLI
=================

This guide shows how to use `bucket-select` to inspect bucket files,
filter data, and export results. It focuses on practical commands and explains
the output fields you will see.

Quick start
-----------

Get file overview:
```
bucket-select info --file jan-check_data/buckets.parquet
```

Get a few random buckets:
```
bucket-select random --file jan-check_data/buckets.parquet -n 3
```

Filter by time range:
```
bucket-select by-time --file jan-check_data/buckets.parquet \
  --start "2026-01-23 17:30" --end "2026-01-24 02:45" --format json
```

Common options (all commands)
-----------------------------
- `--file, -f`: Parquet file to read (default: `buckets.parquet`).
- `--format`: Output format: `compact`, `normal`, `detailed`, `json`, `csv`.
- `--output-file, -o`: Write output to a file (best with `--format json`).
- `--limit`: Limit number of results displayed.

Time formats
------------
- Accepts ISO timestamps or simple strings: `"2026-01-23 17:30"`,
  `"2026-01-23T17:30:00Z"`.

Output formats
--------------
- `compact`: single-line summary per bucket.
- `normal`: multi-section output (default).
- `detailed`: full bucket detail, including templates and component counts.
- `json`: machine-readable output.
- `csv`: only for template instance/timeline commands.

Tip: `--output-file` supports JSON outputs for bucket lists and template
analytics. For other formats, use console output.

Commands
========

count
-----
Show total number of buckets in a file.
```
bucket-select count --file jan-check_data/buckets.parquet
```

Example JSON output:
```
{"total_buckets": 687}
```

event-count
-----------
Show total number of events across all buckets.
```
bucket-select event-count --file jan-check_data/buckets.parquet
```

Example JSON output:
```
{"total_events": 1512}
```

info
----
Show file overview (bucket count + time range).
```
bucket-select info --file jan-check_data/buckets.parquet --format json
```

Example output:
```
{
  "file": "jan-check_data/buckets.parquet",
  "total_buckets": 687,
  "time_range": {
    "start": "2026-01-12T23:15:00",
    "end": "2026-01-26T19:35:00"
  }
}
```
Fields:
- `file`: input file path
- `total_buckets`: number of bucket rows
- `time_range.start/end`: earliest and latest bucket timestamps

random
------
Random sample of buckets (good for sanity checks).
```
bucket-select random --file jan-check_data/buckets.parquet -n 3
```

Example output (compact):
```
╭────── Query Results ──────╮
│                           │
│   📊 Found    3 buckets   │
│   📋 Format   compact     │
│                           │
╰───────────────────────────╯

b_20260115_1445 events=1 errors=0 warnings=0 spike=No w_latency=N/A
b_20260119_1515 events=1 errors=0 warnings=0 spike=No w_latency=N/A
b_20260122_2045 events=1 errors=0 warnings=0 spike=No w_latency=N/A
```
What it means:
- The header summarizes how many buckets matched and the display format.
- Each line shows a single bucket with a quick metric summary.

top / bottom
------------
Sort by a numeric field and return the top or bottom N.
```
bucket-select top --file jan-check_data/buckets.parquet \
  --field write_latency_ms -n 5
```

```
bucket-select bottom --file jan-check_data/buckets.parquet \
  --field total_events -n 5
```

Example output (top, compact):
```
╭────── Query Results ──────╮
│                           │
│   📊 Found    5 buckets   │
│   📋 Format   compact     │
│                           │
╰───────────────────────────╯

b_20260112_2315 events=112 errors=0 warnings=0 spike=No w_latency=N/A
b_20260112_2320 events=6 errors=0 warnings=0 spike=No w_latency=N/A
b_20260112_2345 events=1 errors=0 warnings=0 spike=No w_latency=N/A
b_20260113_0015 events=1 errors=0 warnings=0 spike=No w_latency=N/A
b_20260113_0045 events=1 errors=0 warnings=0 spike=No w_latency=N/A
```

Example output (bottom, compact):
```
╭────── Query Results ──────╮
│                           │
│   📊 Found    5 buckets   │
│   📋 Format   compact     │
│                           │
╰───────────────────────────╯

b_20260112_2345 events=1 errors=0 warnings=0 spike=No w_latency=N/A
b_20260113_0015 events=1 errors=0 warnings=0 spike=No w_latency=N/A
b_20260113_0045 events=1 errors=0 warnings=0 spike=No w_latency=N/A
b_20260113_0115 events=1 errors=0 warnings=0 spike=No w_latency=N/A
b_20260113_0145 events=1 errors=0 warnings=0 spike=No w_latency=N/A
```
What it means:
- Each line shows the bucket ID and quick metrics.
- For `top`, the first line is the largest value in the selected field.
- For `bottom`, the first line is the smallest value in the selected field.

Common fields you can sort on:
- `total_events`, `error_count`, `warning_count`, `info_count`
- `read_latency_ms`, `write_latency_ms`
- `cpu_util`, `memory_util`, `disk_util`

by-flags
--------
Filter buckets by boolean flags.
```
bucket-select by-flags --file jan-check_data/buckets.parquet \
  --flag has_spike=false --format compact --limit 3
```

Example output (compact):
```
╭────── Query Results ──────╮
│                           │
│   📊 Found    3 buckets   │
│   📋 Format   compact     │
│                           │
╰───────────────────────────╯

b_20260112_2315 events=112 errors=0 warnings=0 spike=No w_latency=N/A
b_20260112_2320 events=6 errors=0 warnings=0 spike=No w_latency=N/A
b_20260112_2345 events=1 errors=0 warnings=0 spike=No w_latency=N/A
```
What it means:
- Results only include buckets where the flag expression is true.
- Compact output shows the bucket ID and a short metric summary.

Flag values:
`true|false|1|0|yes|no|on|off`

by-thresholds
-------------
Filter by numeric thresholds using `gt`, `gte`, `lt`, `lte`.
```
bucket-select by-thresholds --file jan-check_data/buckets.parquet \
  --total-events-gt 5 --format detailed --limit 1
```

Example output (detailed, truncated):
```
╭──────────────────────────────────────────────────────────────────────────────╮
│                         🟢 DETAILED BUCKET ANALYSIS                          │
╰──────────────────────────────────────────────────────────────────────────────╯
╭────────────── Metrics ───────────────╮╭──────── Events & Components ─────────╮
│ No performance data                  ││      Event Breakdown                 │
│ ╭──── 🖥️ System Resources ─────╮      ││         ╷       ╷                    │
│ │ No system metrics available │      ││   Level │ Count │   Rate             │
│ ╰─────────────────────────────╯      ││ ╶───────┼───────┼────────╴           │
│                                      ││   Info  │   112 │ 100.0%             │
│                                      ││         ╵       ╵                    │
│                                      ││ 📦 Component Activity                │
│                                      ││ ├── Resources: 64 events             │
│                                      ││ ├── HighAvailability: 18 events      │
│                                      ││ ├── LocalService: 5 events           │
│                                      ││ ├── License: 5 events                │
╰──────────────────────────────────────╯╰──────────────────────────────────────╯
                                  📋 Templates                                  
╭────┬───────┬─────────────────────────────────────────────────────────────────╮
│ ID │ Count │ Pattern                                                         │
├────┼───────┼─────────────────────────────────────────────────────────────────┤
│ 41 │    30 │ Trying to open device 1051: /run/s1-disks/1051                  │
│ 42 │    30 │ Registering device 1034 (/dev/sdac3) with pools [3:All,         │
│    │       │ 1065:pool1 (Manual), 2:SSD]                                     │
│ 14 │     3 │ Current State: NoRepresentative                                 │
│ 10 │     2 │ Initializing...                                                 │
```
What it means:
- The detailed view shows metrics, event breakdown, component activity, and templates.
- The Templates table lists template IDs, counts, and the template string.
- Some sections may show "No data" if the bucket lacks those metrics.

Supported threshold fields (examples):
- `write-latency-ms-gt`
- `read-latency-ms-gte`
- `error-count-gt`
- `total-events-gte`
- `cpu-util-lt`

by-time
-------
Filter buckets by time range between the given start and end timestamps.
```
bucket-select by-time --file jan-check_data/buckets.parquet \
  --start "2026-01-23 17:30" --end "2026-01-24 02:45"
```

Example output (JSON, truncated):
```
[
  {
    "bucket_id": "b_20260123_1750",
    "start_time": "2026-01-23T17:50:00",
    "end_time": "2026-01-23T17:55:00",
    "has_spike": false,
    "total_events": 1,
    "error_count": 0,
    "warning_count": 0,
    "info_count": 1,
    "templates": {
      "2": {
        "template_str": "CheckRebuildSpaceForNotifications started, with stress level: <*> Is parallel: <*>",
        "instances": [
          {"var1": "1500.", "var2": "True", "ts": "2026-01-23T17:54:35+00:00"}
        ]
      }
    }
  }
]
```
What it means:
- Output is a list of buckets with all available fields.
- `templates` contains per-template details and instances in the bucket.
- Use `--format json --output-file` to save the full payload.

Template analytics
==================

templates instances
-------------------
List individual template instances.
```
bucket-select templates instances --file jan-check_data/buckets.parquet \
  --template-id 2 --start "2026-01-23 17:30" --end "2026-01-24 02:45" \
  --include-vars --format csv
```

Example output (CSV, truncated):
```
bucket_id,start_time,end_time,template_id,template_str,instance_index,timestamp,var2,var1
b_20260123_1750,2026-01-23 17:50:00,2026-01-23 17:55:00,2,"CheckRebuildSpaceForNotifications started, with stress level: <*> Is parallel: <*>",0,2026-01-23T17:54:35+00:00,True,1500.
b_20260123_1825,2026-01-23 18:25:00,2026-01-23 18:30:00,2,"CheckRebuildSpaceForNotifications started, with stress level: <*> Is parallel: <*>",0,2026-01-23T18:28:52+00:00,True,1500.
```
What it means:
- Each row is a single template instance occurrence.
- `timestamp` is the exact event time for that instance.
- `var1..varN` columns contain template parameters when `--include-vars` is used.

Output fields:
- `bucket_id`, `start_time`, `end_time`
- `template_id`, `template_str`
- `instance_index` (position within the bucket)
- `var1..var5` when `--include-vars` is used

templates frequency
-------------------
Count how often templates appear in the selected buckets.
```
bucket-select templates frequency --file jan-check_data/buckets.parquet \
  --top-n 10 --format json
```

Example output:
```
{
  "2": 662,
  "57": 513,
  "58": 87,
  "59": 42,
  "60": 42
}
```
What it means:
- Keys are template IDs.
- Values are total occurrences across the filtered buckets.

You can also compare cohorts:
```
bucket-select templates frequency --file jan-check_data/buckets.parquet \
  --compare-cohort high_error --compare-cohort low_error \
  --cohort-filter high_error:error_count=5 \
  --cohort-filter low_error:error_count=0
```

templates timeline
------------------
Export time-series rows combining bucket metrics and template instances.
```
bucket-select templates timeline --file jan-check_data/buckets.parquet \
  --template-id 2 --format csv
```

Example output (CSV, truncated):
```
bucket_id,start_time,end_time,total_events,error_count,warning_count,info_count,cpu_util,memory_util,disk_util,read_latency_ms,write_latency_ms,has_spike,rebuild_active,template_id,template_str,instance_index,timestamp,var1,var2
b_20260112_2345,2026-01-12 23:45:00+00:00,2026-01-12 23:50:00+00:00,1,0,0,1,,,,,,False,,2,"CheckRebuildSpaceForNotifications started, with stress level: <*> Is parallel: <*>",0,2026-01-12T23:49:53+00:00,1.,False
b_20260113_0015,2026-01-13 00:15:00+00:00,2026-01-13 00:20:00+00:00,1,0,0,1,,,,,,False,,2,"CheckRebuildSpaceForNotifications started, with stress level: <*> Is parallel: <*>",0,2026-01-13T00:19:53+00:00,1.,False
```
What it means:
- Each row combines bucket metrics with a template instance.
- Useful for plotting template activity alongside system metrics.

templates precursors
--------------------
Find templates that appear before an anchor template.
```
bucket-select templates precursors --file jan-check_data/buckets.parquet \
  --anchor 2 --lookback 10 --format json
```

Example output:
```
[]
```
What it means:
- An empty list means no precursor templates were found in the lookback window.

Output field glossary
=====================

Bucket fields (common)
----------------------
- `bucket_id`: time-aligned bucket identifier.
- `start_time`, `end_time`: bucket time range.
- `total_events`: total log lines in the bucket.
- `error_count`, `warning_count`, `info_count`: severity counts.
- `read_latency_ms`, `write_latency_ms`: latency metrics.
- `read_iops`, `write_iops`: I/O operations per second.
- `cpu_util`, `memory_util`, `disk_util`: system utilization.
- `has_spike`: true if latency spike detected.
- `rebuild_active`, `degraded_volumes`: rebuild health signals.
- `templates`: template data for that bucket (full output in detailed mode).

Template fields (instances/timeline)
------------------------------------
- `template_id`: template identifier.
- `template_str`: canonical message template.
- `instance_index`: occurrence index in the bucket.
- `var1..var5`: template variable values (if included).

Troubleshooting
---------------
- Empty output: try removing filters or add `--format json` to inspect raw fields.
- Unexpected time range: verify timestamps in the file with `info`.
