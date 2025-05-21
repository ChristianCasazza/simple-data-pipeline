# File: datafusion_example.py
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "datafusion>=42.0.0",
#   "requests"
# ]
# ///

"""
DataFusion Script writing all outputs into “data/outputs” under the given base path.

Usage:
  uv run --python 3.11 datafusion_example.py /some/base/path
  => Writes to /some/base/path/data/outputs/

or if no path is provided:
  => Writes to (script’s dir)/data/outputs/
"""

import sys
import requests
from pathlib import Path
from datafusion import SessionContext


class DataFusionWrapper:
    def __init__(self):
        self.con = SessionContext()
        self.registered_tables = []

    def register_data(self, paths, table_names):
        if len(paths) != len(table_names):
            raise ValueError("The number of paths must match the number of table names.")

        for path, table_name in zip(paths, table_names):
            file_extension = Path(path).suffix.lower()
            if file_extension == ".parquet":
                self.con.register_parquet(table_name, str(path))
            elif file_extension == ".csv":
                self.con.register_csv(table_name, str(path))
            elif file_extension == ".json":
                self.con.register_json(table_name, str(path))
            else:
                raise ValueError(f"Unsupported file type '{file_extension}' for file: {path}")
            self.registered_tables.append(table_name)

    def run_query(self, sql_query):
        return self.con.sql(sql_query)

    def export(self, df, file_type, path=None, with_header=True):
        file_type = file_type.lower()
        if file_type not in ["parquet", "csv", "json"]:
            raise ValueError("file_type must be one of 'parquet', 'csv', or 'json'.")
        output_path = Path(path).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if file_type == "csv":
            df.write_csv(output_path, with_header=with_header)
        elif file_type == "json":
            df.write_json(output_path)
        elif file_type == "parquet":
            df.write_parquet(output_path)

        print(f"File written to: {output_path}")


def main(base_path=None):
    # Determine base_path
    if base_path is None and len(sys.argv) > 1:
        base_path = sys.argv[1]
    base_path = Path(base_path or Path(__file__).parent).resolve()

    # TEMP raw data directory (if you still want the raw parquet)
    raw_dir = base_path / "data"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # OUTPUT directory for all final files
    output_dir = base_path / "data" / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Using output directory: {output_dir}")

    # Download raw parquet if needed
    mta_url = "https://fastopendata.org/mta_subway_hourly_ridership/year%3D2024/month%3D01/mta_subway_hourly_ridership_202401_1.parquet"
    local_parquet = raw_dir / "mta_subway_hourly_ridership.parquet"
    if not local_parquet.exists():
        print(f"Downloading Parquet file from {mta_url}…")
        resp = requests.get(mta_url)
        resp.raise_for_status()
        local_parquet.write_bytes(resp.content)
        print("Download complete!")

    # Run queries
    con = DataFusionWrapper()
    con.register_data([local_parquet], ["mta_hourly_subway"])

    # 1) weekly riders
    q1 = """
        SELECT
          station_complex,
          DATE_TRUNC('week', transit_timestamp) AS week_start,
          SUM(ridership) AS total_weekly_ridership
        FROM mta_hourly_subway
        GROUP BY station_complex, DATE_TRUNC('week', transit_timestamp)
        ORDER BY station_complex, week_start
    """
    df1 = con.run_query(q1)
    con.export(df1, file_type="csv", path=output_dir / "weekly_riders.csv")

    # 2) ridership by borough
    q2 = """
        SELECT borough, SUM(ridership) AS total_ridership
        FROM mta_hourly_subway
        GROUP BY borough
        ORDER BY total_ridership DESC
    """
    df2 = con.run_query(q2)
    con.export(df2, file_type="csv", path=output_dir / "ridership_by_borough.csv")

    # 3) top stations
    q3 = """
        SELECT station_complex, SUM(ridership) AS total_ridership
        FROM mta_hourly_subway
        GROUP BY station_complex
        ORDER BY total_ridership DESC
        LIMIT 5
    """
    df3 = con.run_query(q3)
    con.export(df3, file_type="csv", path=output_dir / "top_stations.csv")

    print("\nAll queries executed and files saved successfully!")


if __name__ == "__main__":
    main()
