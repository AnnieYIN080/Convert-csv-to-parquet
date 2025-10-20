import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# 1. Boilerplate setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 2. Define S3 Paths
csv_input_path = "s3://your-input-bucket/large_file.csv"
parquet_output_path = "s3://your-output-bucket/parquet_data_glue/"

# 3. Read the data using Glue's Data Catalog (recommended for large files)
# If the CSV is not in the Data Catalog, you can use "spark.read.csv".
data_source = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [csv_input_path]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
)

# 4. Write the DynamicFrame to Parquet format in S3
glueContext.write_dynamic_frame.from_options(
    frame=data_source,
    connection_type="s3",
    connection_options={"path": parquet_output_path},
    format="parquet" # Glue automatically handles the conversion and partitioning
)

job.commit()
