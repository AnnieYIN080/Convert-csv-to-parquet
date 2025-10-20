import pandas as pd
import s3fs # Required to read/write directly to S3 with Pandas/PyArrow

# 1. Define S3 Paths
csv_input_path = "s3://your-input-bucket/medium_file.csv"
parquet_output_path = "s3://your-output-bucket/single_parquet_file.parquet"

# 2. Read the CSV file directly from S3
# Note: This loads the entire file into memory.
df = pd.read_csv(
    csv_input_path,
    storage_options={'client': s3fs.S3FileSystem()} # Connect via s3fs
)

# 3. Write to Parquet format in S3
df.to_parquet(
    parquet_output_path, 
    engine='pyarrow', # Use the fast PyArrow engine
    index=False,
    storage_options={'client': s3fs.S3FileSystem()}
)
