# log_anylizer

Quickstart for installing the package and then running the CLI workflows using
the guides in `UserDocs/`.

## Quickstart

### Install git + pip (if missing)

Ubuntu/Debian:
```bash
sudo apt update
sudo apt install -y git python3-pip python3-venv
```

macOS (Homebrew):
```bash
brew install git python
```

### Clone the repo

```bash
git clone https://github.com/amitflank/Log_Anylizer.git
cd Log_Anylizer
```

### Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

### Set up CLI aliases (used in UserDocs)

```bash
alias bucket-build='python -m log_ingest.cli_commands.cli bucket create'
alias bucket-select='python -m log_ingest.cli_commands.cli select-buckets'
alias shap='python -m gbdt_pipeline.cli shap'
```

## Run the CLI (follow UserDocs)

Start with the overview in `UserDocs/README.md`, then use the specific guides:

- Bucket creation: `UserDocs/bucket_create_cli.md`
- Bucket selection: `UserDocs/bucket_select_cli.md`
- SHAP analysis: `UserDocs/shap_analysis_cli.md`

