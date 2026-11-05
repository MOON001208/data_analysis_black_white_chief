"""Refresh the portfolio trend snapshot from the project's Supabase table.

Requires SUPABASE_URL and SUPABASE_KEY in the environment or root .env.
The credentials are never written to the CSV.
"""

import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent.parent
OUTPUT = Path(__file__).resolve().parent / "data" / "trend_snapshot.csv"


def main():
    load_dotenv(ROOT / ".env")
    url = os.environ["SUPABASE_URL"].rstrip("/") + "/rest/v1/chief_trend_value"
    key = os.environ["SUPABASE_KEY"]
    rows = []
    for source in ("google", "youtube", "datalab"):
        for start in range(0, 10000, 1000):
            response = requests.get(
                url,
                headers={
                    "apikey": key,
                    "Authorization": f"Bearer {key}",
                    "Range": f"{start}-{start + 999}",
                },
                params={
                    "select": "출연자,날짜,값,소스",
                    "소스": f"eq.{source}",
                    "order": "출연자.asc,날짜.asc",
                },
                timeout=30,
            )
            response.raise_for_status()
            batch = response.json()
            rows.extend(batch)
            if len(batch) < 1000:
                break

    df = pd.DataFrame(rows).rename(
        columns={"출연자": "Chef", "날짜": "Date", "값": "Value", "소스": "Source"}
    )
    df["Chef"] = df["Chef"].replace("술 빚는 윤주모", "윤주모")
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df["Value"] = pd.to_numeric(df["Value"], errors="raise")
    # Google and YouTube have three samples per chef/day; seaborn previously
    # averaged them implicitly when drawing the line. Store that mean once.
    df = df.groupby(["Chef", "Date", "Source"], as_index=False)["Value"].mean()
    df = df.sort_values(["Chef", "Source", "Date"])
    df.to_csv(OUTPUT, index=False, encoding="utf-8")
    print(f"Saved {len(df)} trend rows to {OUTPUT}")


if __name__ == "__main__":
    main()
