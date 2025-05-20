# Use official slim Python 3.11 image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir duckdb requests

# Copy your script into the image
COPY duckdb_example.py .

# Default command: run the script
ENTRYPOINT ["python", "duckdb_example.py"]
