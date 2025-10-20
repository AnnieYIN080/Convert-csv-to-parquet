# S3 CSV to Parquet Conversion Guide (Python) <br>
This repository provides robust, scalable Python scripts to efficiently convert large CSV files stored in AWS S3 into the highly optimized Parquet format. <br>

Conversion method choice depends heavily on **file size** and **computational environment**.<br>

| Method | Best For | Environment | Key Feature |
| :--- | :--- | :--- | :--- |
| **Pandas / PyArrow** | Small/Medium (up to a few GB) | Single powerful instance (e.g., EC2) | Fastest I/O for in-memory data. |
| **Dask** | Large (tens of GB, > RAM) | Single instance (powerful, high RAM) | Uses **out-of-core** processing (chunks) to handle files larger than memory. |
| **PySpark / AWS Glue** | Massive (hundreds of GBs to TBs) | Distributed Cluster (AWS EMR, Databricks) | Handles true distributed computing and horizontal scaling. |


1. Medium Scale: Pandas/PyArrow (In-Memory)
`pip install pandas s3fs pyarrow`
This method is ideal when the entire CSV file fits comfortably in your machine's RAM. (Suitable for Single Machine)<br>

If your file is large (e.g., up to a few GBs) but small enough to fit within the memory limits of a single powerful machine (like a high-end EC2 instance), you can use the Pandas ecosystem enhanced by PyArrow.<br>

PyArrow is the underlying engine for Parquet files and is much faster than traditional Pandas I/O. For files slightly larger than memory, you can use an intermediate approach called Dask which utilizes PyArrow for chunked processing.<br>


2. Large Scale: Dask (Out-of-Core Processing)
`pip install dask[dataframe] s3fs pyarrow`
Dask is excellent for files too large for a single machine's memory, as it automatically breaks the data into chunks, processes them in parallel, and manages memory usage.


4. Massive Scale: PySpark / AWS Glue (Distributed Computing)<br>
For true petabyte-scale data, a horizontally scalable cluster framework is required.<br>

3.1 PySpark (AWS EMR / Databricks)<br>
For files that are hundreds of GBs or TBs, running this conversion on a cluster like AWS EMR or Databricks using PySpark is the most robust method. PySpark handles parallelism and memory management automatically.<br>

3.2 AWS Glue (Serverless ETL)<br>
AWS Glue abstracts much of the Spark setup, providing a serverless platform for this conversion. It is the preferred method when managing a cluster is undesired.<br>
If you prefer a fully serverless solution and are already using AWS, AWS Glue (which uses Spark internally) is designed for exactly this kind of ETL (Extract, Transform, Load) job. You simply define the source and target in a Glue Studio job or a Python Shell script.<br>
