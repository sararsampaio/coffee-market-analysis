"""SQL analysis execution utilities."""

from pathlib import Path
import sqlite3

import pandas as pd


def parse_sql_file(sql_file: Path) -> list[str]:
    """Parse SQL script into executable SELECT statements."""
    raw = sql_file.read_text(encoding="utf-8")
    non_comment_lines = [line for line in raw.splitlines() if not line.strip().startswith("--")]
    cleaned = "\n".join(non_comment_lines)
    statements = [part.strip() for part in cleaned.split(";")]
    return [statement for statement in statements if statement]


def run_queries(database_path: Path, queries: list[str]) -> list[pd.DataFrame]:
    """Execute SQL queries and return dataframes."""
    with sqlite3.connect(database_path) as conn:
        return [pd.read_sql_query(query, conn) for query in queries]


def export_results(results: list[pd.DataFrame], output_dir: Path, prefix: str = "query") -> None:
    """Export SQL results to CSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for index, frame in enumerate(results, start=1):
        frame.to_csv(output_dir / f"{prefix}_{index:02d}.csv", index=False)
