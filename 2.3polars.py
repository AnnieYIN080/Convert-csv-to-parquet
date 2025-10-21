import polars as pl
import s3fs

S3_CSV_INPUT = "s3://your-input-bucket/super_large_dataset.csv"
S3_PARQUET_OUTPUT = "s3://your-output-bucket/processed_data/filtered_output.parquet"
S3_CSV_OUTPUT = "s3://your-output-bucket/processed_data/filtered_output.csv"

s3 = s3fs.S3FileSystem()

# ---  Use Polars (recommended: Lazy/declarative Processing) ---

try:
    # pl.scan_csv() enables lazy mode to avoid loading the entire file at once.
    # Polars only reads and process the required columns.
    lazy_df = pl.scan_csv(
        S3_CSV_INPUT,
        # storage_options={'aws_access_key_id': '', 'aws_secret_access_key': ''} 
    )

    filtered_lazy = lazy_df.select(
        pl.col("ColA"),
        pl.col("ColB"),
        (pl.col("ColA") + 1).alias("NewCol")
    ).filter(
        pl.col("ColB") > 100 
    )

    print(filtered_lazy.collect_schema())

    # --- save to Parquet / csv ---
    filtered_lazy.collect().write_parquet(    # Or read_csv
        S3_PARQUET_OUTPUT,
        row_group_size=500000, 
        compression="snappy"
    )
   
except Exception as e:
    print(f"Polars failed: {e}")
