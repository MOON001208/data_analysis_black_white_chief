"""Rebuild the small, frozen datasets used by the portfolio dashboard.

Run from any directory with: python 대시보드용/prepare_portfolio_data.py
"""

from pathlib import Path

import pandas as pd

from data_processor import calculate_review_changes, load_reviews


DATA_DIR = Path(__file__).resolve().parent / "data"


def main():
    reviews = load_reviews()
    changes = calculate_review_changes(reviews)
    # A percent change is undefined when the pre-broadcast count is zero.
    changes.loc[changes['before_count'] == 0, 'change_rate'] = float('nan')
    changes.to_csv(DATA_DIR / "review_changes.csv", index=False, encoding="utf-8")

    raw = pd.read_csv(
        DATA_DIR / "seoul_floating_pop_raw3.csv",
        usecols=["SENSING_TIME", "AUTONOMOUS_DISTRICT", "VISITOR_COUNT"],
        parse_dates=["SENSING_TIME"],
    )
    raw["date"] = raw["SENSING_TIME"].dt.date
    daily = raw.groupby(["date", "AUTONOMOUS_DISTRICT"], as_index=False).agg(
        population=("VISITOR_COUNT", "sum"),
        observations=("VISITOR_COUNT", "size"),
    )
    daily = daily.rename(columns={"AUTONOMOUS_DISTRICT": "district"})
    daily.to_csv(DATA_DIR / "daily_population_by_district.csv", index=False, encoding="utf-8")

    print(f"Review summary: {len(changes)} rows")
    print(f"Population summary: {len(daily)} rows, {daily['population'].sum():,} visits")


if __name__ == "__main__":
    main()
