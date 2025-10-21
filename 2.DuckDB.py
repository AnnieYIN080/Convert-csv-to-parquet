import duckdb
import os 
# PyArrow is automatically used by DuckDB to read Parquet/CSV

# 1. Define S3 Paths
csv_input_path = "s3://your-input-bucket/large_file.csv"
parquet_output_path = "s3://your-output-bucket/partitioned_parquet_files"

# 2. Use DuckDB to read the CSV and write Parquet directly (Streaming)
# DuckDB streams the data chunk-by-chunk, minimizing RAM usage.
try:
    # Initialize DuckDB connection (in-memory or persistent)
    con = duckdb.connect(database=':memory:', read_only=False)

    # Note: DuckDB automatically handles S3 credentials/access if s3fs is configured
    # Or you can set S3 access key environment variables before running.

    # 3. SQL command to read CSV and write Parquet, partitioned
    # 'COPY ... TO' is the most efficient way to convert data in DuckDB.
    con.sql(f"""
        COPY (
            SELECT *
            FROM read_csv_auto('{csv_input_path}', all_varchar=FALSE)
        )
        TO '{parquet_output_path}' (
            FORMAT PARQUET,
            ROW_GROUP_SIZE 100000,
            PARTITION_BY year_column, -- Optional: Partition for better query performance later
            OVERWRITE TRUE
        );
    """)
    print(f"Successfully streamed CSV to partitioned Parquet files at: {parquet_output_path}")

except Exception as e:
    print(f"An error occurred during DuckDB conversion: {e}")
finally:
    con.close()

# This method avoids loading the entire file into the local Pandas DataFrame.
