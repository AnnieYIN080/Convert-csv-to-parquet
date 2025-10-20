import dask.dataframe as dd
import s3fs

# Define S3 Paths
csv_input_path = "s3://your-input-bucket/large_file.csv"
# Dask typically writes out multiple partitioned Parquet files
parquet_output_path = "s3://your-output-bucket/dask_parquet_data/"

# Initialize S3 filesystem client
fs = s3fs.S3FileSystem()

# 1. Read the CSV file using Dask
# Dask reads lazily; it knows how to read the file but doesn't load it yet.
# 'blocksize' can be adjusted to control chunk size (e.g., 64MB)
ddf = dd.read_csv(
    csv_input_path,
    storage_options={'client': fs},
    blocksize='64MB' 
)

# 2. Convert and write the Dask DataFrame to Parquet in S3
# .compute() or .persist() triggers the actual reading and writing.
ddf.to_parquet(
    parquet_output_path,
    engine='pyarrow',
    write_metadata_file=True, # Recommended for Dask output
    overwrite=True 
)
