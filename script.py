#!/usr/bin/env python3
"""A small but professional data engineering workflow.

The script reads a CSV file, applies validation and cleaning, writes staged
outputs (bronze/silver/gold), writes Parquet copies, loads the curated data
into SQLite, and emits a run report for observability.

Example:
	python script.py --input data/raw/sales.csv --output-dir data/processed \
		--primary-key order_id --date-column order_date
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sqlite3
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LOGGER = logging.getLogger("data_pipeline")


@dataclass(frozen=True)
class PipelineConfig:
	input_path: Path
	output_dir: Path
	primary_key: str | None = None
	date_column: str | None = None
	mandatory_columns: tuple[str, ...] = ()
	warehouse_path: Path | None = None
	enable_parquet: bool = True
	enable_sql_load: bool = True


@dataclass
class PipelineStats:
	source_file: str
	input_rows: int = 0
	bronze_rows: int = 0
	silver_rows: int = 0
	gold_rows: int = 0
	duplicate_rows_removed: int = 0
	rejected_rows: int = 0
	missing_columns: list[str] = None  # type: ignore[assignment]
	null_counts: dict[str, int] = None  # type: ignore[assignment]
	started_at: str = ""
	finished_at: str = ""

	def __post_init__(self) -> None:
		if self.missing_columns is None:
			self.missing_columns = []
		if self.null_counts is None:
			self.null_counts = {}


def parse_args() -> PipelineConfig:
	parser = argparse.ArgumentParser(
		description="Run a professional CSV data engineering workflow."
	)
	parser.add_argument("--input", required=True, help="Path to the source CSV file")
	parser.add_argument(
		"--output-dir",
		required=True,
		help="Directory where bronze, silver, gold, and reports are written",
	)
	parser.add_argument(
		"--primary-key",
		default=None,
		help="Optional column used to deduplicate records",
	)
	parser.add_argument(
		"--date-column",
		default=None,
		help="Optional column used for date normalization and gold aggregation",
	)
	parser.add_argument(
		"--mandatory-columns",
		default="",
		help="Comma-separated list of columns that must be present and non-empty",
	)
	parser.add_argument(
		"--warehouse-path",
		default=None,
		help="Optional SQLite file used for loading curated datasets",
	)
	parser.add_argument(
		"--no-parquet",
		action="store_true",
		help="Disable Parquet exports",
	)
	parser.add_argument(
		"--no-sql-load",
		action="store_true",
		help="Disable SQLite loading",
	)

	args = parser.parse_args()
	mandatory_columns = tuple(
		column.strip() for column in args.mandatory_columns.split(",") if column.strip()
	)
	warehouse_path = (
		Path(args.warehouse_path).expanduser().resolve()
		if args.warehouse_path
		else None
	)

	return PipelineConfig(
		input_path=Path(args.input).expanduser().resolve(),
		output_dir=Path(args.output_dir).expanduser().resolve(),
		primary_key=args.primary_key,
		date_column=args.date_column,
		mandatory_columns=mandatory_columns,
		warehouse_path=warehouse_path,
		enable_parquet=not args.no_parquet,
		enable_sql_load=not args.no_sql_load,
	)


def setup_logging() -> None:
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
	)


def snake_case(name: str) -> str:
	cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip())
	cleaned = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", cleaned)
	cleaned = re.sub(r"_+", "_", cleaned)
	return cleaned.strip("_").lower()


def normalize_value(value: str | None) -> str | None:
	if value is None:
		return None
	normalized = value.strip()
	if normalized == "":
		return None
	return normalized


def parse_iso_date(value: str | None) -> str | None:
	if value is None:
		return None

	candidate = value.strip()
	if not candidate:
		return None

	formats = ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S")
	for fmt in formats:
		try:
			return datetime.strptime(candidate, fmt).date().isoformat()
		except ValueError:
			continue

	try:
		return datetime.fromisoformat(candidate).date().isoformat()
	except ValueError:
		return candidate


def ensure_directories(output_dir: Path) -> dict[str, Path]:
	stage_dirs = {
		"bronze": output_dir / "bronze",
		"silver": output_dir / "silver",
		"gold": output_dir / "gold",
		"reports": output_dir / "reports",
		"warehouse": output_dir / "warehouse",
	}
	for path in stage_dirs.values():
		path.mkdir(parents=True, exist_ok=True)
	return stage_dirs


def read_csv(input_path: Path) -> list[dict[str, str]]:
	with input_path.open(newline="", encoding="utf-8-sig") as handle:
		return list(csv.DictReader(handle))


def standardize_columns(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, str]]:
	if not records:
		return [], {}

	original_columns = list(records[0].keys())
	mapping: dict[str, str] = {}
	used_names: set[str] = set()

	for column in original_columns:
		candidate = snake_case(column)
		suffix = 1
		unique_name = candidate
		while unique_name in used_names:
			suffix += 1
			unique_name = f"{candidate}_{suffix}"
		used_names.add(unique_name)
		mapping[column] = unique_name

	standardized_records: list[dict[str, Any]] = []
	for record in records:
		standardized: dict[str, Any] = {}
		for original_column, value in record.items():
			standardized[mapping[original_column]] = normalize_value(value)
		standardized_records.append(standardized)

	return standardized_records, mapping


def validate_schema(
	records: list[dict[str, Any]],
	config: PipelineConfig,
) -> tuple[list[str], list[str]]:
	if not records:
		raise ValueError("Input file is empty or missing a header row")

	available_columns = set(records[0].keys())
	required_columns = set(
		snake_case(column) for column in config.mandatory_columns
	)
	missing_columns = sorted(required_columns - available_columns)

	absent_for_quality: list[str] = []
	for column in required_columns:
		if column not in available_columns:
			continue
		if all(record.get(column) is None for record in records):
			absent_for_quality.append(column)

	return missing_columns, absent_for_quality


def write_csv(path: Path, records: list[dict[str, Any]], fieldnames: list[str]) -> None:
	with path.open("w", newline="", encoding="utf-8") as handle:
		writer = csv.DictWriter(handle, fieldnames=fieldnames)
		writer.writeheader()
		for record in records:
			writer.writerow({field: record.get(field) for field in fieldnames})


def write_parquet(path: Path, records: list[dict[str, Any]]) -> None:
	try:
		import pandas as pd
	except ImportError as exc:  # pragma: no cover - defensive guard
		raise RuntimeError(
			"Parquet export requires pandas and pyarrow in the active environment"
		) from exc

	df = pd.DataFrame(records)
	df.to_parquet(path, index=False)


def infer_sqlite_type(values: list[Any]) -> str:
	for value in values:
		if value is None:
			continue
		if isinstance(value, (list, dict, tuple, set)):
			return "TEXT"
		if isinstance(value, bool):
			return "INTEGER"
		if isinstance(value, int):
			return "INTEGER"
		if isinstance(value, float):
			return "REAL"
		return "TEXT"
	return "TEXT"


def sqlite_value(value: Any) -> Any:
	if value is None:
		return None
	if isinstance(value, bool):
		return int(value)
	if isinstance(value, (int, float, str)):
		return value
	if isinstance(value, datetime):
		return value.isoformat()
	if isinstance(value, (list, dict, tuple, set)):
		return json.dumps(value, sort_keys=True)
	return str(value)


def load_records_to_sqlite(db_path: Path, table_name: str, records: list[dict[str, Any]]) -> None:
	db_path.parent.mkdir(parents=True, exist_ok=True)
	with sqlite3.connect(db_path) as connection:
		cursor = connection.cursor()
		cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')

		if not records:
			cursor.execute(f'CREATE TABLE "{table_name}" (empty TEXT)')
			connection.commit()
			return

		fieldnames = list(records[0].keys())
		column_types = {
			field: infer_sqlite_type([record.get(field) for record in records])
			for field in fieldnames
		}
		column_sql = ", ".join(f'"{field}" {column_types[field]}' for field in fieldnames)
		cursor.execute(f'CREATE TABLE "{table_name}" ({column_sql})')

		placeholders = ", ".join("?" for _ in fieldnames)
		insert_sql = f'INSERT INTO "{table_name}" ({", ".join(f"\"{field}\"" for field in fieldnames)}) VALUES ({placeholders})'
		rows = [tuple(sqlite_value(record.get(field)) for field in fieldnames) for record in records]
		cursor.executemany(insert_sql, rows)
		connection.commit()


def write_stage_artifacts(
	stage_dir: Path,
	base_name: str,
	records: list[dict[str, Any]],
	enable_parquet: bool,
) -> dict[str, Path]:
	artifacts: dict[str, Path] = {}
	csv_path = stage_dir / f"{base_name}.csv"
	if records:
		write_csv(csv_path, records, list(records[0].keys()))
	else:
		csv_path.write_text("", encoding="utf-8")
	artifacts["csv"] = csv_path

	if enable_parquet:
		parquet_path = stage_dir / f"{base_name}.parquet"
		write_parquet(parquet_path, records)
		artifacts["parquet"] = parquet_path

	return artifacts


def deduplicate_records(
	records: list[dict[str, Any]],
	primary_key: str | None,
) -> tuple[list[dict[str, Any]], int]:
	if not records:
		return [], 0

	if primary_key and primary_key in records[0]:
		seen_keys: set[str] = set()
		unique_records: list[dict[str, Any]] = []
		duplicate_count = 0

		for record in records:
			key = record.get(primary_key)
			if key is None:
				unique_records.append(record)
				continue
			if key in seen_keys:
				duplicate_count += 1
				continue
			seen_keys.add(key)
			unique_records.append(record)

		return unique_records, duplicate_count

	seen_rows: set[tuple[tuple[str, Any], ...]] = set()
	unique_records = []
	duplicate_count = 0
	for record in records:
		signature = tuple(sorted(record.items()))
		if signature in seen_rows:
			duplicate_count += 1
			continue
		seen_rows.add(signature)
		unique_records.append(record)

	return unique_records, duplicate_count


def normalize_dates(records: list[dict[str, Any]], date_column: str | None) -> None:
	if not date_column:
		return
	for record in records:
		if date_column in record:
			record[date_column] = parse_iso_date(record.get(date_column))


def build_gold_dataset(
	records: list[dict[str, Any]],
	date_column: str | None,
) -> list[dict[str, Any]]:
	if not records:
		return []

	if date_column and date_column in records[0]:
		counts = Counter(record.get(date_column) or "unknown" for record in records)
		return [
			{date_column: date_value, "record_count": count}
			for date_value, count in sorted(counts.items())
		]

	column_nulls = defaultdict(int)
	for record in records:
		for column, value in record.items():
			if value is None:
				column_nulls[column] += 1

	return [
		{
			"metric": "row_count",
			"value": len(records),
		},
		*[
			{"metric": f"null_count::{column}", "value": null_count}
			for column, null_count in sorted(column_nulls.items())
		],
	]


def build_null_counts(records: list[dict[str, Any]]) -> dict[str, int]:
	counts: dict[str, int] = defaultdict(int)
	for record in records:
		for column, value in record.items():
			if value is None:
				counts[column] += 1
	return dict(sorted(counts.items()))


def run_pipeline(config: PipelineConfig) -> PipelineStats:
	if not config.input_path.exists():
		raise FileNotFoundError(f"Input file not found: {config.input_path}")

	LOGGER.info("Reading source file: %s", config.input_path)
	raw_records = read_csv(config.input_path)
	bronze_rows = len(raw_records)

	stage_dirs = ensure_directories(config.output_dir)
	stats = PipelineStats(
		source_file=str(config.input_path),
		input_rows=bronze_rows,
		bronze_rows=bronze_rows,
		started_at=datetime.now(timezone.utc).isoformat(),
	)

	bronze_path = stage_dirs["bronze"] / f"{config.input_path.stem}_bronze.csv"
	bronze_artifacts = write_stage_artifacts(
		stage_dirs["bronze"],
		f"{config.input_path.stem}_bronze",
		raw_records,
		config.enable_parquet,
	)
	bronze_path = bronze_artifacts["csv"]

	standardized_records, column_mapping = standardize_columns(raw_records)
	stats.missing_columns, missing_for_quality = validate_schema(standardized_records, config)

	if stats.missing_columns:
		raise ValueError(
			"Missing required columns after standardization: "
			+ ", ".join(stats.missing_columns)
		)

	if missing_for_quality:
		LOGGER.warning(
			"Required columns exist but are fully null: %s",
			", ".join(missing_for_quality),
		)

	normalized_records = []
	for record in standardized_records:
		cleaned = {column: value for column, value in record.items()}
		normalized_records.append(cleaned)

	if config.date_column:
		normalized_date_column = snake_case(config.date_column)
		normalize_dates(normalized_records, normalized_date_column)
	else:
		normalized_date_column = None

	silver_records, duplicate_count = deduplicate_records(
		normalized_records,
		snake_case(config.primary_key) if config.primary_key else None,
	)

	stats.duplicate_rows_removed = duplicate_count
	stats.silver_rows = len(silver_records)
	stats.rejected_rows = bronze_rows - len(silver_records)
	stats.null_counts = build_null_counts(silver_records)

	silver_artifacts = write_stage_artifacts(
		stage_dirs["silver"],
		f"{config.input_path.stem}_silver",
		silver_records,
		config.enable_parquet,
	)
	silver_path = silver_artifacts["csv"]

	gold_records = build_gold_dataset(silver_records, normalized_date_column)
	stats.gold_rows = len(gold_records)

	gold_artifacts = write_stage_artifacts(
		stage_dirs["gold"],
		f"{config.input_path.stem}_gold",
		gold_records,
		config.enable_parquet,
	)
	gold_path = gold_artifacts["csv"]

	warehouse_path = config.warehouse_path or (stage_dirs["warehouse"] / "warehouse.db")
	if config.enable_sql_load:
		load_records_to_sqlite(warehouse_path, f"{config.input_path.stem}_bronze", raw_records)
		load_records_to_sqlite(warehouse_path, f"{config.input_path.stem}_silver", silver_records)
		load_records_to_sqlite(warehouse_path, f"{config.input_path.stem}_gold", gold_records)
		load_records_to_sqlite(warehouse_path, f"{config.input_path.stem}_run_stats", [asdict(stats)])

	report_path = stage_dirs["reports"] / f"{config.input_path.stem}_run_report.json"
	stats.finished_at = datetime.now(timezone.utc).isoformat()
	report_payload = {
		"config": {
			**asdict(config),
			"input_path": str(config.input_path),
			"output_dir": str(config.output_dir),
			"warehouse_path": str(warehouse_path),
		},
		"column_mapping": column_mapping,
		"stats": asdict(stats),
		"artifacts": {
			"bronze": {name: str(path) for name, path in bronze_artifacts.items()},
			"silver": {name: str(path) for name, path in silver_artifacts.items()},
			"gold": {name: str(path) for name, path in gold_artifacts.items()},
			"warehouse_db": str(warehouse_path) if config.enable_sql_load else None,
		},
	}
	report_path.write_text(json.dumps(report_payload, indent=2), encoding="utf-8")

	LOGGER.info("Wrote bronze dataset to %s", bronze_path)
	LOGGER.info("Wrote silver dataset to %s", silver_path)
	LOGGER.info("Wrote gold dataset to %s", gold_path)
	if config.enable_sql_load:
		LOGGER.info("Loaded datasets into SQLite database %s", warehouse_path)
	LOGGER.info("Wrote run report to %s", report_path)

	return stats


def main() -> int:
	setup_logging()
	config = parse_args()

	try:
		stats = run_pipeline(config)
	except Exception as exc:  # pragma: no cover - top-level failure path
		LOGGER.exception("Pipeline failed: %s", exc)
		return 1

	LOGGER.info(
		"Pipeline completed successfully: input_rows=%s, silver_rows=%s, gold_rows=%s",
		stats.input_rows,
		stats.silver_rows,
		stats.gold_rows,
	)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
