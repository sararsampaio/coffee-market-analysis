import sqlite3

import pandas as pd

from coffee_market.analysis import export_results, parse_sql_file, run_queries


def test_parse_sql_file_splits_statements_and_drops_comments(tmp_path):
    sql_file = tmp_path / "queries.sql"
    sql_file.write_text(
        "-- 1. First query\nSELECT 1 AS a;\n\n-- 2. Second query\nSELECT\n    2 AS b;\n",
        encoding="utf-8",
    )

    assert parse_sql_file(sql_file) == ["SELECT 1 AS a", "SELECT\n    2 AS b"]


def test_run_queries_and_export_results(tmp_path):
    database = tmp_path / "test.db"
    with sqlite3.connect(database) as conn:
        pd.DataFrame({"city": ["Tucson", "Tucson", "Vail"]}).to_sql("business", conn, index=False)

    results = run_queries(database, ["SELECT city, COUNT(*) AS n FROM business GROUP BY city ORDER BY n DESC"])
    export_results(results, tmp_path / "out")

    exported = pd.read_csv(tmp_path / "out" / "query_01.csv")
    assert exported.to_dict("records") == [{"city": "Tucson", "n": 2}, {"city": "Vail", "n": 1}]


def test_project_sql_file_parses_into_five_queries():
    from pathlib import Path

    queries = parse_sql_file(Path(__file__).parents[1] / "sql" / "analysis_queries.sql")

    assert len(queries) == 5
    assert all(query.upper().startswith("SELECT") for query in queries)
