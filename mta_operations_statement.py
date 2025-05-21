# File: mta_operations_statement.py
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
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        headers = {"X-App-Token": self.api_token}
        try:
            resp = session.get(endpoint, params=query_params, headers=headers, timeout=(10, 30))
            resp.raise_for_status()
        finally:
            session.close()
        return resp.json()


def process_mta_operations_statement_df(df: pl.DataFrame) -> (pl.DataFrame, list, list):
    orig_cols = df.columns
    df = df.rename({c: c.lower().replace(" ", "_").replace("-", "_") for c in df.columns})
    df = df.rename({"month": "timestamp"})
    renamed_cols = df.columns
    if "timestamp" in df.columns:
        df = df.with_columns([
            pl.col("timestamp")
              .str.strptime(pl.Date, "%Y-%m-%dT%H:%M:%S%.f", strict=False)
              .alias("timestamp")
        ])
    agency_sql = """
    SELECT *, CASE 
      WHEN agency = 'LIRR' THEN 'Long Island Rail Road'
      -- … etc …
      ELSE 'Unknown Agency'
    END AS agency_full_name
    FROM self
    """
    df = df.sql(agency_sql)
    casts = [
        ("fiscal_year", pl.Int64),
        ("amount", pl.Float64),
        # … etc …
    ]
    exprs = [pl.col(c).cast(t) for c, t in casts if c in df.columns]
    if exprs:
        df = df.with_columns(exprs)
    return df, orig_cols, renamed_cols


def main():
    token = os.getenv("SOCRATA_API_TOKEN")
    if not token:
        print("ERROR: set SOCRATA_API_TOKEN in your environment", file=sys.stderr)
        sys.exit(1)

    soc = SocrataResource(api_token=token)
    endpoint = "https://data.ny.gov/resource/yg77-3tkj.json"
    limit, offset = 500_000, 0
    frames, last_orig, last_renamed = [], [], []

    while True:
        params = {"$limit": limit, "$offset": offset, "$order": "Month ASC"}
        data = soc.fetch_data(endpoint, params)
        if not data:
            break
        df = pl.DataFrame(data)
        proc, orig, renamed = process_mta_operations_statement_df(df)
        frames.append(proc)
        last_orig, last_renamed = orig, renamed
        offset += limit
        del df, proc, data
        gc.collect()

    final_df = pl.concat(frames, how="vertical") if frames else pl.DataFrame([])
    # write to data/outputs/
    out_dir = os.path.join("data", "outputs")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "mta_operations_statement.parquet")
    final_df.write_parquet(out_path)

    print(f"Wrote {final_df.shape[0]} rows to {out_path}")
    print("Original cols:", last_orig)
    print("Renamed cols:", last_renamed)


if __name__ == "__main__":
    main()
