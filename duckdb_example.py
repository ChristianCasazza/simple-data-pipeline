# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "duckdb",
#   "requests"
# ]
# ///

"""
DuckDB Script (no pandas/pyarrow), saving to base_path/data.

Usage:
  uv run --python 3.11 duckdb_no_pandas_script.py /some/base/path
  => Saves files in /some/base/path/data
  
  or no base path => Saves in (script's directory)/data
"""

import sys
import requests
from pathlib import Path
import duckdb


class DuckDBWrapper:
    def __init__(self, duckdb_path=None):
        if duckdb_path:
            self.con = duckdb.connect(str(duckdb_path), read_only=False)
        else:
            self.con = duckdb.connect(database=':memory:', read_only=False)
        self.registered_tables = []

        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")

    def register_data(self, paths, table_names):
        if len(paths) != len(table_names):
            raise ValueError("Number of paths must match number of table names.")

        for path, table_name in zip(paths, table_names):
            p = str(path)
            ext = Path(p).suffix.lower()

            if ext == ".parquet":
                query = f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{p}')"
            elif ext == ".csv":
                query = f"CREATE VIEW {table_name} AS SELECT * FROM read_csv_auto('{p}')"
            elif ext == ".json":
                query = f"CREATE VIEW {table_name} AS SELECT * FROM read_json_auto('{p}')"
            else:
                raise ValueError(f"Unsupported file type '{ext}' for file: {p}")

            self.con.execute(query)
            self.registered_tables.append(table_name)

    def run_query(self, sql_query):
        """Returns list of tuples for the result."""
        return self.con.execute(sql_query).fetchall()

    def _construct_path(self, path, base_path, file_name, extension):
        if path:
            return Path(path)
        elif base_path and file_name:
            return Path(base_path) / f"{file_name}.{extension}"
        else:
            return Path(f"output.{extension}")

    def export_query(self, sql_query, file_type, path=None, base_path=None, file_name=None, with_header=True):
        file_type = file_type.lower()
        if file_type not in ["csv", "json", "parquet"]:
            raise ValueError("file_type must be one of 'csv', 'json', or 'parquet'.")

        out_path = self._construct_path(path, base_path, file_name, file_type)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if file_type == "csv":
            copy_cmd = (
                f"COPY ({sql_query}) TO '{out_path}' "
                f"(FORMAT 'csv', HEADER {str(with_header).upper()});"
            )
        elif file_type == "json":
            copy_cmd = (
                f"COPY ({sql_query}) TO '{out_path}' (FORMAT 'json');"
            )
        else:  # parquet
            copy_cmd = (
                f"COPY ({sql_query}) TO '{out_path}' (FORMAT 'parquet');"
            )

        self.con.execute(copy_cmd)
        print(f"File written to: {out_path}")


def main(base_path=None):
    # 1) base_path argument
    # 2) sys.argv[1]
    # 3) script's directory
    if base_path is None and len(sys.argv) > 1:
        base_path = sys.argv[1]

    if base_path is None:
        base_path = Path(__file__).parent
    else:
        base_path = Path(base_path).resolve()

    # Use base_path/data as the final directory
    data_dir = base_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"Using base output directory: {data_dir}")

    # Download parquet file if needed
    parquet_url = "https://fastopendata.org/mta/raw/hourly_subway/year%3D2024/month%3D09/2024_09.parquet"
    local_parquet_path = data_dir / "2024_09.parquet"
    if not local_parquet_path.exists():
        print(f"Downloading Parquet file from {parquet_url}...")
        resp = requests.get(parquet_url)
        resp.raise_for_status()
        local_parquet_path.write_bytes(resp.content)
        print("Download complete!")

    # Initialize DuckDB
    db = DuckDBWrapper()

    # Register
    db.register_data([local_parquet_path], ["mta_hourly_subway"])

    # Query 1
    query_weekly_riders = """
        SELECT
            station_complex,
            DATE_TRUNC('week', transit_timestamp)::DATE AS week_start,
            SUM(ridership) AS total_weekly_ridership
        FROM mta_hourly_subway
        GROUP BY
            station_complex,
            DATE_TRUNC('week', transit_timestamp)::DATE
        ORDER BY
            station_complex,
            week_start
    """
    db.export_query(
        query_weekly_riders,
        file_type="csv",
        path=data_dir / "weekly_riders.csv",
        with_header=True
    )

    # Query 2
    query_ridership_by_borough = """
        SELECT
            borough,
            SUM(ridership) AS total_ridership
        FROM mta_hourly_subway
        GROUP BY
            borough
        ORDER BY
            total_ridership DESC
    """
    db.export_query(
        query_ridership_by_borough,
        file_type="csv",
        path=data_dir / "ridership_by_borough.csv",
        with_header=True
    )

    # Query 3
    query_top_stations = """
        SELECT
            station_complex,
            SUM(ridership) AS total_ridership
        FROM mta_hourly_subway
        GROUP BY
            station_complex
        ORDER BY
            total_ridership DESC
        LIMIT 5
    """
    db.export_query(
        query_top_stations,
        file_type="csv",
        path=data_dir / "top_stations.csv",
        with_header=True
    )

    print("\nAll queries executed and files saved successfully!")


if __name__ == "__main__":
    main()
