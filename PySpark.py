from pyspark.sql import SparkSession

# 1. Initialize Spark Session (Spark will handle configuration on EMR/Databricks)
spark = SparkSession.builder.appName("CSVtoParquetS3").getOrCreate()

# 2. Define S3 Paths
csv_input_path = "s3://your-input-bucket/large_file.csv"
parquet_output_path = "s3://your-output-bucket/parquet_data/"

# 3. Read the CSV file
# Important: 'inferSchema' is slow; it's better to provide a schema if possible.
df = spark.read.csv(
    csv_input_path, 
    header=True, 
    inferSchema=True, 
    # Use 'sep' if your CSV is not comma-separated
    # sep=','
)

# 4. Write the DataFrame to Parquet format in S3
# Spark automatically partitions the Parquet output into multiple files 
# for efficient retrieval.
df.write.parquet(
    parquet_output_path, 
    mode="overwrite" # Change to "append" if necessary
)

# Stop the Spark Session
spark.stop()
