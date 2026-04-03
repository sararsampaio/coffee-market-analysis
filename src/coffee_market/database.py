"""SQLite loading utilities."""

from pathlib import Path
import sqlite3

import pandas as pd

TABLE_FILES = {
    "business": "az_coffee_business.csv",
    "reviews": "az_coffee_reviews.csv",
    "tips": "az_coffee_tips.csv",
    "checkins": "az_coffee_checkins.csv",
    "users": "az_coffee_users.csv",
}


def load_csv_tables(input_dir: Path) -> dict[str, pd.DataFrame]:
    """Load prepared CSV files into memory."""
    return {table: pd.read_csv(input_dir / filename) for table, filename in TABLE_FILES.items()}


def write_sqlite_database(tables: dict[str, pd.DataFrame], database_path: Path) -> None:
    """Write all tables to SQLite."""
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database_path) as conn:
        for table, frame in tables.items():
            frame.to_sql(table, conn, if_exists="replace", index=False)
