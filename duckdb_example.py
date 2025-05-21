# File: duckdb_example.py
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb",
#   "requests"
# ]
# ///

"""
DuckDB Script (no pandas/pyarrow), writing all outputs into data/outputs.

Usage:
  uv run --python 3.11 duckdb_example.py /some/base/path
  => Writes into /some/base/path/data/outputs/
or if no base-path is given:
  => Writes into (script’s dir)/data/outputs/
"""

import sys
import requests
from pathlib import Path
import duckdb


class DuckDBWrapper:
    def __init__(self, duckdb_path=None):
        self.con = duckdb.connect(str(duckdb_path) if duckdb_path else ":memory:", read_only=False)
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")
        self.registered_tables = []

    def register_data(self, paths, table_names):
        if len(paths) != len(table_names):
            raise ValueError("Number of paths must match number of table names.")
        for path, table_name in zip(paths, table_names):
            p = str(path)
            ext = Path(p).suffix.lower()
            if ext == ".parquet":
                sql = f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{p}')"
            elif ext == ".csv":
                sql = f"CREATE VIEW {table_name} AS SELECT * FROM read_csv_auto('{p}')"
            elif ext == ".json":
                sql = f"CREATE VIEW {table_name} AS SELECT * FROM read_json_auto('{p}')"
            else:
                raise ValueError(f"Unsupported file type '{ext}' for file: {p}")
            self.con.execute(sql)
            self.registered_tables.append(table_name)

    def run_query(self, sql_query):
        return self.con.execute(sql_query).fetchall()

    def export_query(self, sql_query, file_type, path=None, with_header=True):
        file_type = file_type.lower()
        if file_type not in ["csv", "json", "parquet"]:
            raise ValueError("file_type must be one of 'csv', 'json', or 'parquet'.")
        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if file_type == "csv":
            cmd = (
                f"COPY ({sql_query}) TO '{out_path}' "
                f"(FORMAT 'csv', HEADER {str(with_header).upper()});"
            )
        elif file_type == "json":
            cmd = f"COPY ({sql_query}) TO '{out_path}' (FORMAT 'json');"
        else:  # parquet
            cmd = f"COPY ({sql_query}) TO '{out_path}' (FORMAT 'parquet');"

        self.con.execute(cmd)
        print(f"File written to: {out_path}")


def main(base_path=None):
    # Determine base path
    if base_path is None and len(sys.argv) > 1:
        base_path = sys.argv[1]
    base_path = Path(base_path or Path(__file__).parent).resolve()

    # Keep a raw data dir if you want
    raw_dir = base_path / "data"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # OUTPUT dir for all CSV/parquet
    output_dir = base_path / "data" / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Using output directory: {output_dir}")

    # Download parquet if needed
    url = "https://fastopendata.org/mta_subway_hourly_ridership/year%3D2024/month%3D01/mta_subway_hourly_ridership_202401_1.parquet"
    local_parquet = raw_dir / "mta_subway_hourly_ridership.parquet"
    if not local_parquet.exists():
        print(f"Downloading Parquet file from {url}…")
        resp = requests.get(url)
        resp.raise_for_status()
        local_parquet.write_bytes(resp.content)
        print("Download complete!")

    # Run queries
    db = DuckDBWrapper()
    db.register_data([local_parquet], ["mta_hourly_subway"])

    q1 = """
        SELECT station_complex,
               DATE_TRUNC('week', transit_timestamp)::DATE AS week_start,
               SUM(ridership) AS total_weekly_ridership
        FROM mta_hourly_subway
        GROUP BY station_complex, DATE_TRUNC('week', transit_timestamp)::DATE
        ORDER BY station_complex, week_start
    """
    db.export_query(q1, file_type="csv", path=output_dir / "weekly_riders.csv")

    q2 = """
        SELECT borough, SUM(ridership) AS total_ridership
        FROM mta_hourly_subway
        GROUP BY borough
        ORDER BY total_ridership DESC
    """
    db.export_query(q2, file_type="csv", path=output_dir / "ridership_by_borough.csv")

    q3 = """
        SELECT station_complex, SUM(ridership) AS total_ridership
        FROM mta_hourly_subway
        GROUP BY station_complex
        ORDER BY total_ridership DESC
        LIMIT 5
    """
    db.export_query(q3, file_type="csv", path=output_dir / "top_stations.csv")

    print("\nAll queries executed and files saved successfully!")


if __name__ == "__main__":
    main()
