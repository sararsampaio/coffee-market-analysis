"""Unified CLI entrypoint for the coffee market pipeline."""

import argparse
from pathlib import Path

from .analysis import export_results, parse_sql_file, run_queries
from .database import load_csv_tables, write_sqlite_database
from .preprocessing import build_filtered_data, save_filtered_data
from .visualization import save_standard_plots


def build_parser() -> argparse.ArgumentParser:
    """Build CLI parser with subcommands."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    preprocess = subparsers.add_parser("preprocess", help="Filter raw Yelp JSONL into processed CSVs.")
    preprocess.add_argument("--input-dir", type=Path, required=True, help="Directory with Yelp JSONL files.")
    preprocess.add_argument("--output-dir", type=Path, default=Path("data/processed"), help="CSV output directory.")
    preprocess.add_argument("--chunk-size", type=int, default=50_000, help="JSONL chunk size for large tables.")
    preprocess.set_defaults(handler=handle_preprocess)

    build_db = subparsers.add_parser("build-db", help="Build SQLite database from processed CSVs.")
    build_db.add_argument("--input-dir", type=Path, default=Path("data/processed"), help="Directory with prepared CSV files.")
    build_db.add_argument("--db-path", type=Path, default=Path("data/coffee_analysis.db"), help="SQLite output file.")
    build_db.set_defaults(handler=handle_build_db)

    analyze = subparsers.add_parser("analyze", help="Run SQL analysis and export query result CSVs.")
    analyze.add_argument("--db-path", type=Path, default=Path("data/coffee_analysis.db"), help="SQLite database file.")
    analyze.add_argument("--sql-file", type=Path, default=Path("sql/analysis_queries.sql"), help="SQL script with SELECT queries.")
    analyze.add_argument("--output-dir", type=Path, default=Path("reports/sql_results"), help="Directory for query CSV outputs.")
    analyze.set_defaults(handler=handle_analyze)

    visualize = subparsers.add_parser("visualize", help="Generate standard chart images from SQLite data.")
    visualize.add_argument("--db-path", type=Path, default=Path("data/coffee_analysis.db"), help="SQLite database file.")
    visualize.add_argument("--output-dir", type=Path, default=Path("reports/figures"), help="Directory for generated plots.")
    visualize.set_defaults(handler=handle_visualize)

    return parser


def handle_preprocess(args: argparse.Namespace) -> None:
    """Run preprocess command."""
    data = build_filtered_data(input_dir=args.input_dir, chunk_size=args.chunk_size)
    save_filtered_data(data=data, output_dir=args.output_dir)
    print(f"Saved filtered CSV files to: {args.output_dir}")


def handle_build_db(args: argparse.Namespace) -> None:
    """Run build-db command."""
    tables = load_csv_tables(args.input_dir)
    write_sqlite_database(tables=tables, database_path=args.db_path)
    print(f"SQLite database written to: {args.db_path}")


def handle_analyze(args: argparse.Namespace) -> None:
    """Run analyze command."""
    queries = parse_sql_file(args.sql_file)
    results = run_queries(args.db_path, queries)
    export_results(results, output_dir=args.output_dir)
    print(f"Executed {len(results)} queries. Results saved to: {args.output_dir}")


def handle_visualize(args: argparse.Namespace) -> None:
    """Run visualize command."""
    files = save_standard_plots(database_path=args.db_path, output_dir=args.output_dir)
    print(f"Generated {len(files)} plots in: {args.output_dir}")


def main() -> None:
    """Execute CLI command."""
    parser = build_parser()
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
