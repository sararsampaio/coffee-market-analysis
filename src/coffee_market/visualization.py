"""Chart generation utilities for market analysis outputs."""

from pathlib import Path
import sqlite3

import matplotlib as mpl
import pandas as pd

mpl.use("Agg")
import matplotlib.pyplot as plt


def load_plot_datasets(database_path: Path) -> dict[str, pd.DataFrame]:
    """Load datasets required by standard project visualizations."""
    with sqlite3.connect(database_path) as conn:
        top_cities = pd.read_sql_query(
            """
            SELECT city, COUNT(*) AS total_coffee_shops
            FROM business
            GROUP BY city
            ORDER BY total_coffee_shops DESC
            LIMIT 10;
            """,
            conn,
        )
        city_ratings = pd.read_sql_query(
            """
            SELECT city,
                   COUNT(*) AS total_shops,
                   ROUND(AVG(stars), 2) AS avg_rating
            FROM business
            GROUP BY city
            HAVING COUNT(*) >= 3
            ORDER BY total_shops DESC
            LIMIT 10;
            """,
            conn,
        )
        ratings_vs_reviews = pd.read_sql_query(
            "SELECT review_count, stars FROM business;",
            conn,
        )
    return {
        "top_cities": top_cities,
        "city_ratings": city_ratings,
        "ratings_vs_reviews": ratings_vs_reviews,
    }


def save_standard_plots(database_path: Path, output_dir: Path) -> list[Path]:
    """Generate and save standard analysis charts."""
    output_dir.mkdir(parents=True, exist_ok=True)
    datasets = load_plot_datasets(database_path)
    generated = [
        plot_top_cities(datasets["top_cities"], output_dir / "top_cities_by_shop_count.png"),
        plot_city_ratings(datasets["city_ratings"], output_dir / "average_rating_by_city.png"),
        plot_reviews_vs_rating(datasets["ratings_vs_reviews"], output_dir / "review_count_vs_rating.png"),
    ]
    return generated


def plot_top_cities(frame: pd.DataFrame, output_path: Path) -> Path:
    """Plot top cities by number of coffee shops."""
    fig, ax = plt.subplots(figsize=(10, 5))
    frame.plot(kind="bar", x="city", y="total_coffee_shops", legend=False, ax=ax)
    ax.set_title("Top Cities by Number of Coffee Shops")
    ax.set_xlabel("City")
    ax.set_ylabel("Number of Coffee Shops")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path


def plot_city_ratings(frame: pd.DataFrame, output_path: Path) -> Path:
    """Plot average rating for the largest coffee-shop markets."""
    fig, ax = plt.subplots(figsize=(10, 5))
    frame.plot(kind="bar", x="city", y="avg_rating", legend=False, ax=ax)
    ax.set_title("Average Rating by City")
    ax.set_xlabel("City")
    ax.set_ylabel("Average Rating")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path


def plot_reviews_vs_rating(frame: pd.DataFrame, output_path: Path) -> Path:
    """Plot relationship between review volume and rating."""
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(frame["review_count"], frame["stars"], alpha=0.6)
    ax.set_title("Review Count vs Business Rating")
    ax.set_xlabel("Review Count")
    ax.set_ylabel("Stars")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path
