Bucket Creation CLI
===================

This guide shows how to build `buckets.parquet` using a bucket creation config
file and the `bucket-build` alias.

Quick start
-----------
1) Start from the example config: `config/bucket_create.yaml`
2) Update `data_dir` to the folder that contains your raw logs
3) Run the build:
```
bucket-build --config config/bucket_create.yaml
```

Alias used in this guide:
```
alias bucket-build='python3 -m log_ingest.cli_commands.cli bucket create'
```

Config fields (what to change)
------------------------------

Here is the current example config:
```
data_dir: jan6
event_patterns:
- '*NAS*'
- '*PlatformClient*'
- '*PlatformManager*'
- '*Statistics*'
- '*console.log*'
latency_pattern:
- '*latency*'
system_pattern:
- '*Information*'
file_limit: 80
bucket_duration: 5
spike_threshold: 100.0
output_root: jan-data/
```

Field-by-field guidance
-----------------------

`data_dir` (required - must set)
- Path to the directory containing your raw log files.
- This is the most important field to update for a new dataset.

`event_patterns` (avoid changing)
- Globs that decide which log files are treated as event logs.
- These should rarely be changed; the defaults are tuned to the log layout.

`latency_pattern` (avoid changing)
- Globs used to find latency log files.
- Leave as-is unless your latency files are named differently.

`system_pattern` (avoid changing)
- Globs used to find system/metadata log files.
- Leave as-is in normal usage.

`file_limit` (OK to change)
- Max number of files to process.
- Defaults to 500 in the CLI; increase only if you need more coverage.


`bucket_duration` (avoid changing)
- Bucket size in minutes (default 5).
- Changing this affects bucekt length shouldnt impact much if you do but for most part leave as-is.

`spike_threshold` (OK to change)
- Threshold for marking latency spikes.
- Currently heuristic; will be dynamically calculated later.
- You can leave this unchanged.

`output_root` (OK to change)
- Output folder for generated artifacts.
- Change if you want buckets written somewhere else.
- bucket file will be created at output_root/buckets/buckets.parquet

What gets produced
------------------
Running the command creates an output folder with:
- `buckets.parquet` (the main output)
- Additional metadata and cache artifacts (depending on config)

Example
-------
```
bucket-build --config config/bucket_create.yaml
```
After completion, look under `output_root` for `buckets.parquet`.
