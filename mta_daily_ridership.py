# File: mta_daily_ridership.py
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "requests>=2.28.0",
#   "polars>=0.18.0",
#   "python-dotenv>=1.0.0",
# ]
# ///

import os
import sys
import gc
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import polars as pl
from typing import Any, Dict, List

load_dotenv()


class SocrataResource:
    def __init__(self, api_token: str):
        self.api_token = api_token

    def fetch_data(self, endpoint: str, query_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        headers = {"X-App-Token": self.api_token}
        try:
            resp = session.get(endpoint, params=query_params, headers=headers, timeout=(10, 30))
            resp.raise_for_status()
        finally:
            session.close()
        if endpoint.endswith(".geojson"):
            feats = resp.json().get("features", [])
            return [f.get("properties", {}) for f in feats]
        return resp.json()


def process_mta_daily_df(df: pl.DataFrame) -> (pl.DataFrame, list, list, str):
    orig_cols = df.columns
    df = df.rename({c: c.lower().replace(" ", "_") for c in df.columns})
    renamed_cols = df.columns
    date_sample = "N/A"
    if "date" in df.columns:
        df = df.with_columns(
            pl.col("date")
              .str.strptime(pl.Date, "%Y-%m-%dT%H:%M:%S%.f", strict=False)
              .alias("date")
        )
        date_sample = str(df.select("date").head(3).to_dicts())
    old_new = [
        ("subways_total_estimated_ridership", "subways_total_ridership"),
        # … other renames …
    ]
    exprs, drops = [], []
    for old, new in old_new:
        if old in df.columns:
            exprs.append(pl.col(old).cast(pl.Float64).alias(new))
            drops.append(old)
    if exprs:
        df = df.with_columns(exprs).drop(drops)
    return df, orig_cols, renamed_cols, date_sample


def main():
    token = os.getenv("SOCRATA_API_TOKEN")
    if not token:
        print("ERROR: set SOCRATA_API_TOKEN in your environment", file=sys.stderr)
        sys.exit(1)

    soc = SocrataResource(api_token=token)
    endpoint = "https://data.ny.gov/resource/vxuj-8kew.json"
    limit, offset = 500_000, 0
    frames, last_orig, last_renamed, date_sample = [], [], [], "N/A"

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "Date ASC",
            "$where": "Date >= '2020-03-01T00:00:00'",
        }
        data = soc.fetch_data(endpoint, params)
        if not data:
            break
        df = pl.DataFrame(data)
        proc, orig, renamed, sample = process_mta_daily_df(df)
        frames.append(proc)
        last_orig, last_renamed, date_sample = orig, renamed, sample
        offset += limit
        del df, proc, data
        gc.collect()

    final_df = pl.concat(frames, how="vertical") if frames else pl.DataFrame([])
    # write to data/outputs/
    out_dir = os.path.join("data", "outputs")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "mta_daily_ridership.parquet")
    final_df.write_parquet(out_path)

    print(f"Wrote {final_df.shape[0]} rows to {out_path}")
    print("Sample dates:", date_sample)
    print("Original cols:", last_orig)
    print("Renamed cols:", last_renamed)


if __name__ == "__main__":
    main()
