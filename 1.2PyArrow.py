# --------------------------Reading from Amazon S3 and Writing Parquet--------------------------------
import s3fs
import fsspec
# The fsspec and s3fs libraries are needed to enable PyArrow to access remote files (like S3) by providing a file-like object or filesystem interface.

fs = s3fs.S3FileSystem()
# Initialize the S3FileSystem object. This handles AWS authentication (e.g., via IAM roles or environment variables).

import pyarrow.csv as pv
import pyarrow.parquet as pq
# Import the necessary PyArrow modules for CSV reading and Parquet writing.

with fs.open(s3_path_csv, 'rb') as f:
    # The 'rb' (read binary) mode is used as required by PyArrow for efficient reading.
    table = pv.read_csv(f)
    
# At this point, the 'table' object holds the data in columnar format in memory.

with fs.open(s3_path_parquet, 'wb') as f_out:
    pq.write_table(table, f_out, compression='snappy')

# --------------------------Reading from Local Disk and Writing Parquet (Local)--------------------------------
import pyarrow.csv as pv
import pyarrow.parquet as pq

input_csv_path = '/content/sample_data/california_housing_test.csv'
output_parquet_path_direct = '/content/sample_data/california_housing_test.parquet'
# Define local input and output file paths.

# Read the CSV file directly into a PyArrow Table (Arrow-Native I/O).
table = pv.read_csv(input_csv_path)

# Write the PyArrow Table directly to a Parquet file.
pq.write_table(
    table,
    output_parquet_path_direct,
    # The 'compression' argument defines the codec used in the Parquet file.
    # 'snappy' is the recommended default for balanced speed and size.
    # Alternatives: 'zstd' (highest compression ratio) or 'gzip' (widest compatibility, slowest).
    compression='snappy',
)
