# Data Engineering Pipeline

A professional CSV data engineering workflow that ingests raw data, validates and cleans it, writes staged outputs (bronze/silver/gold), exports to Parquet, loads into SQLite, and generates a run report.

## Features

- **CSV Ingestion**: Reads raw CSV files with flexible encoding support
- **Data Validation**: Enforces required columns and checks for null values
- **Data Cleaning**: Normalizes column names to snake_case, trims whitespace, normalizes dates
- **Deduplication**: Removes duplicate records based on primary key or row signature
- **Multi-Format Output**: 
  - Bronze/Silver/Gold CSV files
  - Parquet files (compressed columnar format)
  - SQLite warehouse with typed tables
- **Run Report**: JSON report with configuration, statistics, and artifact paths

## Quick Start

### 1. Set Up Environment

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Run the Pipeline

```bash
python script.py \
  --input data/raw/orders.csv \
  --output-dir data/processed \
  --primary-key "Order ID" \
  --date-column "Order Date" \
  --mandatory-columns "Order ID,Order Date"
```

### 3. View the Output

The pipeline creates a directory structure under `data/processed/`:

```
data/processed/
├── bronze/
│   ├── orders_bronze.csv
│   └── orders_bronze.parquet
├── silver/
│   ├── orders_silver.csv
│   └── orders_silver.parquet
├── gold/
│   ├── orders_gold.csv
│   └── orders_gold.parquet
├── reports/
│   └── orders_run_report.json
└── warehouse/
    └── warehouse.db
```

### 4. Inspect the Run Report

```bash
# Pretty-print the JSON report
python3 -c "import json; print(json.dumps(json.load(open('data/processed/reports/orders_run_report.json')), indent=2))"
```

### 5. Query the SQLite Warehouse

```bash
sqlite3 data/processed/warehouse/warehouse.db
# Then run SQL like:
# SELECT * FROM orders_silver;
# SELECT * FROM orders_gold;
# SELECT * FROM orders_run_stats;
```

## CLI Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--input` | Yes | Path to input CSV file |
| `--output-dir` | Yes | Directory for output artifacts |
| `--primary-key` | No | Column name for deduplication |
| `--date-column` | No | Column to normalize as ISO dates |
| `--mandatory-columns` | No | Comma-separated required columns |
| `--warehouse-path` | No | Custom SQLite database path |
| `--no-parquet` | No | Disable Parquet exports |
| `--no-sql-load` | No | Disable SQLite loading |

## Airflow Integration

A sample Airflow DAG is provided at `dags/data_engineering_pipeline_dag.py`. To use it:

1. Install Airflow: `pip install apache-airflow`
2. Copy the DAG to your Airflow dags folder
3. Update the paths in the DAG (INPUT_PATH, OUTPUT_DIR, etc.) to match your environment
4. Trigger the DAG in Airflow UI or CLI

## Data Flow

1. **Bronze**: Raw data as-is, with standardized column names
2. **Silver**: Cleaned data (nulls normalized, duplicates removed)
3. **Gold**: Aggregated metrics (row counts, null counts by column, or date-based summaries)

## Example Session

```bash
# Run the pipeline
python script.py \
  --input data/raw/orders.csv \
  --output-dir data/processed \
  --primary-key "Order ID" \
  --date-column "Order Date"

# View statistics
sqlite3 data/processed/warehouse/warehouse.db "SELECT COUNT(*) FROM orders_silver;"

# Check for duplicates removed
cat data/processed/reports/orders_run_report.json | python3 -m json.tool | grep duplicate_rows_removed
```

## Logging

The pipeline logs to stdout with timestamps and severity levels. Set `PYTHONUNBUFFERED=1` to see real-time output:

```bash
PYTHONUNBUFFERED=1 python script.py --input data/raw/orders.csv --output-dir data/processed
```

## Notes

- All imports are from the Python standard library except pandas/pyarrow (Parquet) and airflow (orchestration)
- Parquet export requires pandas and pyarrow; these are optional and gracefully disabled if not available
- SQLite is auto-created and supports any column name via proper quoting
- Column names are normalized to snake_case; collisions are resolved by adding numeric suffixes
