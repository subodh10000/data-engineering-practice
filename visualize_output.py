#!/usr/bin/env python3
"""Generate charts from a completed pipeline run.

The script discovers the most recent run report, reads the corresponding SQLite
warehouse, and produces PNG charts under a visualizations directory.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


LOGGER = logging.getLogger("pipeline_visualizer")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate charts from a data engineering pipeline run."
    )
    parser.add_argument(
        "--output-dir",
        default="data/processed",
        help="Pipeline output directory containing reports and warehouse folders",
    )
    parser.add_argument(
        "--report-path",
        default=None,
        help="Optional explicit path to a run report JSON file",
    )
    parser.add_argument(
        "--visualization-dir",
        default=None,
        help="Directory where charts should be written",
    )
    return parser.parse_args()


def find_latest_report(output_dir: Path) -> Path:
    reports_dir = output_dir / "reports"
    report_files = sorted(reports_dir.glob("*_run_report.json"), key=lambda path: path.stat().st_mtime)
    if not report_files:
        raise FileNotFoundError(f"No run report found in {reports_dir}")
    return report_files[-1]


def load_report(report_path: Path) -> dict[str, Any]:
    return json.loads(report_path.read_text(encoding="utf-8"))


def open_connection(warehouse_path: Path) -> sqlite3.Connection:
    if not warehouse_path.exists():
        raise FileNotFoundError(f"SQLite warehouse not found: {warehouse_path}")
    return sqlite3.connect(warehouse_path)


def table_row_count(connection: sqlite3.Connection, table_name: str) -> int:
    cursor = connection.execute(f'SELECT COUNT(*) FROM "{table_name}"')
    return int(cursor.fetchone()[0])


def table_columns(connection: sqlite3.Connection, table_name: str) -> list[str]:
    cursor = connection.execute(f'PRAGMA table_info("{table_name}")')
    return [row[1] for row in cursor.fetchall()]


def query_rows(connection: sqlite3.Connection, sql: str) -> list[tuple[Any, ...]]:
    cursor = connection.execute(sql)
    return cursor.fetchall()


def save_bar_chart(output_path: Path, labels: list[str], values: list[int], title: str, ylabel: str) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(labels, values, color="#2f6fed")
    ax.set_title(title, fontsize=15, weight="bold")
    ax.set_ylabel(ylabel)
    ax.set_axisbelow(True)
    ax.grid(axis="y", alpha=0.25)
    plt.xticks(rotation=25, ha="right")
    fig.tight_layout()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def save_line_chart(output_path: Path, labels: list[str], values: list[int], title: str, ylabel: str) -> None:
    fig, ax = plt.subplots(figsize=(12, 6))
    positions = list(range(len(values)))
    ax.plot(positions, values, color="#0f766e", linewidth=2.5, marker="o", markersize=3)
    ax.fill_between(positions, values, color="#0f766e", alpha=0.08)
    ax.set_title(title, fontsize=15, weight="bold")
    ax.set_ylabel(ylabel)
    ax.set_xlabel("Date")
    ax.set_axisbelow(True)
    ax.grid(alpha=0.25)
    ax.set_xticks(positions)
    plt.xticks(rotation=45, ha="right")
    ax.set_xticklabels(labels)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def save_horizontal_bar_chart(output_path: Path, labels: list[str], values: list[int], title: str, xlabel: str) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(labels, values, color="#b45309")
    ax.set_title(title, fontsize=15, weight="bold")
    ax.set_xlabel(xlabel)
    ax.set_axisbelow(True)
    ax.grid(axis="x", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def save_histogram(output_path: Path, values: list[float], title: str, xlabel: str) -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(values, bins=30, color="#7c3aed", edgecolor="white", alpha=0.9)
    ax.set_title(title, fontsize=15, weight="bold")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Frequency")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)


def generate_visualizations(output_dir: Path, report_path: Path, visualization_dir: Path) -> list[Path]:
    report = load_report(report_path)
    warehouse_path = Path(report["config"]["warehouse_path"])
    stem = report_path.stem.replace("_run_report", "")

    stage_tables = {
        "bronze": f"{stem}_bronze",
        "silver": f"{stem}_silver",
        "gold": f"{stem}_gold",
    }

    visualization_dir.mkdir(parents=True, exist_ok=True)
    chart_paths: list[Path] = []

    with open_connection(warehouse_path) as connection:
        bronze_count = table_row_count(connection, stage_tables["bronze"])
        silver_count = table_row_count(connection, stage_tables["silver"])
        gold_count = table_row_count(connection, stage_tables["gold"])

        stage_path = visualization_dir / f"{stem}_stage_counts.png"
        save_bar_chart(
            stage_path,
            ["Bronze", "Silver", "Gold"],
            [bronze_count, silver_count, gold_count],
            "Row Counts by Stage",
            "Rows",
        )
        chart_paths.append(stage_path)

        gold_columns = table_columns(connection, stage_tables["gold"])
        if len(gold_columns) >= 2:
            rows = query_rows(
                connection,
                f'SELECT "{gold_columns[0]}", "{gold_columns[1]}" FROM "{stage_tables["gold"]}" ORDER BY 1',
            )
            if rows:
                labels = [str(row[0]) for row in rows]
                values = [int(row[1]) for row in rows]
                gold_path = visualization_dir / f"{stem}_gold_trend.png"
                save_line_chart(
                    gold_path,
                    labels,
                    values,
                    "Gold Aggregation Trend",
                    "Record Count",
                )
                chart_paths.append(gold_path)

        null_counts = report.get("stats", {}).get("null_counts", {})
        if null_counts:
            ordered = sorted(null_counts.items(), key=lambda item: item[1], reverse=True)
            labels = [item[0] for item in ordered]
            values = [int(item[1]) for item in ordered]
            null_path = visualization_dir / f"{stem}_null_counts.png"
            save_horizontal_bar_chart(
                null_path,
                labels,
                values,
                "Null Counts in Silver",
                "Null Rows",
            )
            chart_paths.append(null_path)

        silver_columns = table_columns(connection, stage_tables["silver"])
        if "amount" in silver_columns:
            amount_rows = query_rows(
                connection,
                f'SELECT CAST("amount" AS REAL) FROM "{stage_tables["silver"]}" WHERE "amount" IS NOT NULL',
            )
            amount_values = [float(row[0]) for row in amount_rows if row[0] is not None]
            if amount_values:
                hist_path = visualization_dir / f"{stem}_amount_distribution.png"
                save_histogram(
                    hist_path,
                    amount_values,
                    "Order Amount Distribution",
                    "Amount",
                )
                chart_paths.append(hist_path)

    summary_path = visualization_dir / f"{stem}_visualization_index.json"
    summary_path.write_text(
        json.dumps(
            {
                "report_path": str(report_path),
                "warehouse_path": str(warehouse_path),
                "charts": [str(path) for path in chart_paths],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    chart_paths.append(summary_path)
    return chart_paths


def main() -> int:
    setup_logging()
    args = parse_args()

    output_dir = Path(args.output_dir).expanduser().resolve()
    if args.report_path:
        report_path = Path(args.report_path).expanduser().resolve()
    else:
        report_path = find_latest_report(output_dir)

    visualization_dir = (
        Path(args.visualization_dir).expanduser().resolve()
        if args.visualization_dir
        else output_dir / "visualizations"
    )

    try:
        chart_paths = generate_visualizations(output_dir, report_path, visualization_dir)
    except Exception as exc:  # pragma: no cover - top-level failure path
        LOGGER.exception("Visualization generation failed: %s", exc)
        return 1

    LOGGER.info("Generated %s visualization artifacts", len(chart_paths))
    for chart_path in chart_paths:
        LOGGER.info("Created %s", chart_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
