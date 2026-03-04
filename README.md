# Data Engineering Assignment

This repository implements the assignment requirements end-to-end:

1. Data extraction from Sunrise-Sunset API
2. ETL into SQLite using a Singer-style message flow
3. Analytical Jupyter notebook for required questions

## What is Implemented

### Required Scope

- Extract sunrise/sunset for a location from `2020-01-01` to current date (or custom date range).
- Convert and store all times in **IST (UTC+5:30)**.
- Ingest into SQLite using Singer-style `SCHEMA` + `RECORD` JSONL messages.
- Deliver analysis in a clean, reproducible notebook.

### Optional Scope (Bonus)

- Visualization scaffold added via Grafana dashboard JSON (`grafana_dashboard.json`).
- Scheduling (Airflow) is intentionally not implemented.

## Project Files

- `pipeline_etl.py` — Main ETL script (extract, transform, Singer messages, SQLite load).
- `analytics.ipynb` — Notebook for required analytics.
- `grafana_dashboard.json` — Optional Grafana dashboard definition.


Generated outputs:

- `data/singer_messages.jsonl` — Singer-style output file.
- `data/sunrise_sunset.db` — SQLite database.
- `data/sunrise_sunset.csv` — CSV export from SQLite table.

## Prerequisites

- Python 3.9+
- Internet access for API calls

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install requests pandas jupyter
```

## Run ETL Pipeline

Basic run:

```bash
python pipeline_etl.py --location "19.0760,72.8777"
```

If your machine has SSL certificate trust issues:

```bash
python pipeline_etl.py --location "19.0760,72.8777" --insecure
```

Full requirement range example:

```bash
python pipeline_etl.py \
  --location "19.0760,72.8777" \
  --start-date 2020-01-01 \
  --end-date 2026-03-03 \
  --insecure
```

What the ETL does:

- Calls API once per date.
- Parses UTC sunrise/sunset and converts to IST.
- Computes `day_length_seconds`.
- Writes Singer-style messages.
- Upserts rows into SQLite (`ON CONFLICT(date) DO UPDATE`).
- Prints progress periodically for long runs.

## Run Analytical Notebook

```bash
jupyter notebook analytics.ipynb
```

Notebook outputs (as required):

1. Longest and shortest day per calendar year
2. Latest sunrise time per month
3. Earliest sunset time per month


## Grafana Visualization (Bonus)

You can import `grafana_dashboard.json` in Grafana and connect it to `data/sunrise_sunset.db` via SQLite datasource plugin to visualize:

- Longest/shortest day trends
- Day-length trend over time
- Latest sunrise and earliest sunset monthly patterns

## Scheduling with Airflow (Optional - Not Implemented)

This assignment item is optional and is currently not implemented in this repository. If implemented, the simplest approach would be a local Airflow DAG that runs `pipeline_etl.py` once per day.

### Overall Scheduling Logic

- Create one DAG scheduled daily (for example: `@daily`).
- Task 1 runs the ETL script using a `BashOperator` (or `PythonOperator`).
- Task 2 can run a quick validation query (row count/date check) to confirm load success.
- Optional Task 3 can export refreshed CSV after load.

### How Current ETL Script Supports Airflow

The existing `pipeline_etl.py` already works well for scheduling because:

- It is CLI-driven (`--start-date`, `--end-date`, `--location`, paths).
- It is idempotent on date key (`ON CONFLICT(date) DO UPDATE`), so reruns are safe.
- It prints clear progress and completion output, useful for Airflow logs.
- It has deterministic file/database outputs, making downstream tasks simple.

### Example Airflow Run Command

In Airflow, a task command would look like:

```bash
python /path/to/pipeline_etl.py \
  --location "19.0760,72.8777" \
  --start-date 2020-01-01 \
  --end-date {{ ds }} \
  --db-path /path/to/data/sunrise_sunset.db \
  --messages-path /path/to/data/singer_messages.jsonl
```


## ETL Framework Note (Singer/Meltano)

This implementation uses a **Singer-style approach directly in Python**:

- Emits `SCHEMA` and `RECORD` messages in JSONL format
- Loads those records into SQLite

Meltano project scaffolding is not included, but the message contract is Singer-compatible and can be migrated into Meltano later if needed.
