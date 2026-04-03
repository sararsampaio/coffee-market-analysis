"""Data preprocessing pipeline for coffee market analysis."""

from pathlib import Path

import pandas as pd

COFFEE_CATEGORY_PATTERN = r"Coffee|Cafe|Tea"

RAW_FILES = {
    "business": "yelp_academic_dataset_business.json",
    "reviews": "yelp_academic_dataset_review.json",
    "tips": "yelp_academic_dataset_tip.json",
    "checkins": "yelp_academic_dataset_checkin.json",
    "users": "yelp_academic_dataset_user.json",
}

BUSINESS_COLUMNS = [
    "business_id",
    "name",
    "city",
    "state",
    "latitude",
    "longitude",
    "stars",
    "review_count",
    "categories",
    "is_open",
]

REVIEW_COLUMNS = [
    "review_id",
    "user_id",
    "business_id",
    "stars",
    "useful",
    "funny",
    "cool",
    "text",
    "date",
]

TIP_COLUMNS = ["user_id", "business_id", "text", "date", "compliment_count"]

USER_COLUMNS = [
    "user_id",
    "review_count",
    "yelping_since",
    "useful",
    "funny",
    "cool",
    "fans",
    "average_stars",
]


def build_filtered_data(input_dir: Path, chunk_size: int = 50_000) -> dict[str, pd.DataFrame]:
    """Build filtered coffee datasets for the configured scope."""
    business = pd.read_json(input_dir / RAW_FILES["business"], lines=True)
    coffee_businesses = filter_businesses(business)
    coffee_business_ids = set(coffee_businesses["business_id"])

    reviews = filter_chunked_file(input_dir / RAW_FILES["reviews"], coffee_business_ids, REVIEW_COLUMNS, chunk_size)
    reviews["date"] = pd.to_datetime(reviews["date"], errors="coerce")
    reviews["review_length"] = reviews["text"].fillna("").str.len()

    tips = filter_chunked_file(input_dir / RAW_FILES["tips"], coffee_business_ids, TIP_COLUMNS, chunk_size)
    tips["date"] = pd.to_datetime(tips["date"], errors="coerce")
    tips["tip_length"] = tips["text"].fillna("").str.len()

    checkins = pd.read_json(input_dir / RAW_FILES["checkins"], lines=True)
    checkins = checkins[checkins["business_id"].isin(coffee_business_ids)].copy()
    checkins["checkin_dates"] = checkins["date"].fillna("")
    checkins["total_checkins"] = checkins["checkin_dates"].apply(count_checkins)

    coffee_user_ids = set(reviews["user_id"].dropna()) | set(tips["user_id"].dropna())
    users = filter_users(input_dir / RAW_FILES["users"], coffee_user_ids, chunk_size)

    return {
        "business": coffee_businesses,
        "reviews": reviews,
        "tips": tips,
        "checkins": checkins,
        "users": users,
    }


def filter_businesses(business: pd.DataFrame) -> pd.DataFrame:
    """Return coffee-related businesses for Arizona."""
    filtered = business[
        (business["state"] == "AZ")
        & (business["categories"].fillna("").str.contains(COFFEE_CATEGORY_PATTERN, case=False, regex=True))
    ].copy()
    return filtered[BUSINESS_COLUMNS]


def filter_chunked_file(
    path: Path,
    business_ids: set[str],
    columns: list[str],
    chunk_size: int,
) -> pd.DataFrame:
    """Filter a large JSONL file by business IDs."""
    chunks: list[pd.DataFrame] = []
    for chunk in pd.read_json(path, lines=True, chunksize=chunk_size):
        filtered = chunk[chunk["business_id"].isin(business_ids)].copy()
        if not filtered.empty:
            chunks.append(filtered[columns])
    if not chunks:
        return pd.DataFrame(columns=columns)
    return pd.concat(chunks, ignore_index=True)


def filter_users(path: Path, user_ids: set[str], chunk_size: int) -> pd.DataFrame:
    """Return user rows referenced by reviews or tips."""
    chunks: list[pd.DataFrame] = []
    for chunk in pd.read_json(path, lines=True, chunksize=chunk_size):
        filtered = chunk[chunk["user_id"].isin(user_ids)].copy()
        if not filtered.empty:
            chunks.append(filtered[USER_COLUMNS])
    if not chunks:
        return pd.DataFrame(columns=USER_COLUMNS)
    return pd.concat(chunks, ignore_index=True)


def count_checkins(value: str) -> int:
    """Count comma-separated check-in timestamps."""
    if not value:
        return 0
    return len([item for item in str(value).split(",") if item.strip()])


def save_filtered_data(data: dict[str, pd.DataFrame], output_dir: Path) -> None:
    """Persist filtered dataframes as CSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    data["business"].to_csv(output_dir / "az_coffee_business.csv", index=False)
    data["reviews"].to_csv(output_dir / "az_coffee_reviews.csv", index=False)
    data["tips"].to_csv(output_dir / "az_coffee_tips.csv", index=False)
    data["checkins"].to_csv(output_dir / "az_coffee_checkins.csv", index=False)
    data["users"].to_csv(output_dir / "az_coffee_users.csv", index=False)
