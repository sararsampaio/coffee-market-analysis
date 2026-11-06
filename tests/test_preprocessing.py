import pandas as pd

from coffee_market.preprocessing import BUSINESS_COLUMNS, count_checkins, filter_businesses


def make_business(rows: list[dict]) -> pd.DataFrame:
    defaults = {column: None for column in BUSINESS_COLUMNS}
    return pd.DataFrame([{**defaults, **row} for row in rows])


def test_keeps_coffee_categories_in_arizona():
    business = make_business(
        [
            {"business_id": "a", "state": "AZ", "city": "Tucson", "categories": "Coffee & Tea, Bakeries"},
            {"business_id": "b", "state": "AZ", "city": "Tucson", "categories": "Restaurants, Cafes"},
            {"business_id": "c", "state": "AZ", "city": "Tucson", "categories": "Bubble Tea"},
            {"business_id": "d", "state": "PA", "city": "Philadelphia", "categories": "Coffee & Tea"},
        ]
    )

    assert list(filter_businesses(business)["business_id"]) == ["a", "b", "c"]


def test_excludes_categories_that_only_contain_tea_as_a_substring():
    business = make_business(
        [
            {"business_id": "steak", "state": "AZ", "city": "Tucson", "categories": "Steakhouses, Restaurants"},
            {"business_id": "cheese", "state": "AZ", "city": "Tucson", "categories": "Cheesesteaks"},
            {"business_id": "music", "state": "AZ", "city": "Tucson", "categories": "Musical Instruments & Teachers"},
            {"business_id": "sport", "state": "AZ", "city": "Tucson", "categories": "Amateur Sports Teams"},
            {"business_id": "canteen", "state": "AZ", "city": "Tucson", "categories": "Cafeteria"},
            {"business_id": "none", "state": "AZ", "city": "Tucson", "categories": None},
        ]
    )

    assert filter_businesses(business).empty


def test_corrects_misspelled_and_padded_city_names():
    business = make_business(
        [
            {"business_id": "a", "state": "AZ", "city": "Maran", "categories": "Cafes"},
            {"business_id": "b", "state": "AZ", "city": " Tucson ", "categories": "Cafes"},
        ]
    )

    assert list(filter_businesses(business)["city"]) == ["Marana", "Tucson"]


def test_returns_business_columns_in_schema_order():
    business = make_business([{"business_id": "a", "state": "AZ", "city": "Tucson", "categories": "Cafes"}])
    business["extra"] = 1

    assert list(filter_businesses(business).columns) == BUSINESS_COLUMNS


def test_count_checkins():
    assert count_checkins("2016-04-26 19:49:16, 2016-08-30 18:36:57, 2017-01-01 10:00:00") == 3
    assert count_checkins("2016-04-26 19:49:16,") == 1
    assert count_checkins("") == 0
