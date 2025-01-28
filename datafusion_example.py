# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "datafusion>=42.0.0",
#   "requests"
# ]
# ///

"""
DataFusion Script with subdirectory "data" inside the base path.

Usage:
  uv run --python 3.11 datafusion_script.py /some/base/path
  => Data saved to /some/base/path/data
  
  or if no path is provided:
  => Data saved to (script's directory)/data
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

        if file_type == "csv":
            df.write_csv(output_path, with_header=with_header)
        elif file_type == "json":
            df.write_json(output_path)
        elif file_type == "parquet":
            df.write_parquet(output_path)

        print(f"File written to: {output_path}")


def main(base_path=None):
    # 1) If the user passed a base_path to main(), use that.
    # 2) Else if there's a CLI argument, use that.
    # 3) Otherwise default to the script directory.
    if base_path is None and len(sys.argv) > 1:
        base_path = sys.argv[1]

    if base_path is None:
        base_path = Path(__file__).parent
    else:
        base_path = Path(base_path).resolve()

    # Now always use "base_path/data" as the final output directory
    data_dir = base_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"Using base output directory: {data_dir}")

    # URL to remote Parquet file
    mta_url = "https://fastopendata.org/mta/raw/hourly_subway/year%3D2024/month%3D09/2024_09.parquet"
    local_parquet_file = data_dir / "2024_09.parquet"

    # Download if needed
    if not local_parquet_file.exists():
        print(f"Downloading Parquet file from {mta_url}...")
        resp = requests.get(mta_url)
        resp.raise_for_status()
        local_parquet_file.write_bytes(resp.content)
        print("Download complete!")

    # Initialize DataFusion
    con = DataFusionWrapper()
    con.register_data([local_parquet_file], ["mta_hourly_subway"])

    # Query 1: weekly_riders
    query_weekly_riders = """
        SELECT
            station_complex,
            DATE_TRUNC('week', transit_timestamp) AS week_start,
            SUM(ridership) AS total_weekly_ridership
        FROM
            mta_hourly_subway
        GROUP BY
            station_complex,
            DATE_TRUNC('week', transit_timestamp)
        ORDER BY
            station_complex,
            week_start
    """
    df_weekly = con.run_query(query_weekly_riders)
    con.export(df_weekly, file_type="csv", path=data_dir / "weekly_riders.csv")

    # Query 2: ridership_by_borough
    query_borough = """
        SELECT
            borough,
            SUM(ridership) AS total_ridership
        FROM
            mta_hourly_subway
        GROUP BY
            borough
        ORDER BY
            total_ridership DESC
    """
    df_borough = con.run_query(query_borough)
    con.export(df_borough, file_type="csv", path=data_dir / "ridership_by_borough.csv")

    # Query 3: top_stations
    query_top_stations = """
        SELECT
            station_complex,
            SUM(ridership) AS total_ridership
        FROM
            mta_hourly_subway
        GROUP BY
            station_complex
        ORDER BY
            total_ridership DESC
        LIMIT 5
    """
    df_top = con.run_query(query_top_stations)
    con.export(df_top, file_type="csv", path=data_dir / "top_stations.csv")

    print("\nAll queries executed and CSV files saved successfully!")


if __name__ == "__main__":
    main()
