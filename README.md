
# Simple Python Scripts for Working with Parquet Files

This repository contains simple Python scripts to demonstrate the following:

1. **Downloading a parquet file from object storage (R2 in this case)**.
2. **Using Datafusion or DuckDB to query the file with SQL**.
3. **Exporting the query results as CSV files**.

## Getting Started

### Clone the Repository
To get started, clone this repository using:
```bash
git clone https://github.com/ChristianCasazza/simple-data-pipeline
cd simple-data-pipeline
```

Alternatively, you can directly copy one of the Python files for your use if using uv.

### Install `uv`
These scripts can be run easier with `uv`, a lightweight Python virtual environment and task runner.

Follow the installation guide for `uv` here: [Installing uv](https://docs.astral.sh/uv/getting-started/installation/).

### Running the Scripts
Once `uv` is installed, you can run the scripts with the following commands:
```bash
uv run duckdb_example.py
uv run datafusion_example.py
```

### Using a Virtual Environment (Optional)
If you prefer to use a virtual environment and pip, you can follow these steps:
1. Create a virtual environment:
   ```bash
   python -m venv .venv
   ```
2. Activate the virtual environment:
   - On Linux/macOS:
     ```bash
     source .venv/bin/activate
     ```
   - On Windows:
     ```bash
     .venv\Scripts\activate
     ```
3. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the scripts:
   ```bash
   python datafusion_example.py
   python duckdb_example.py
   ```

