# S3 CSV to Parquet Conversion Guide (Python) <br>
This repository provides robust, scalable Python scripts to efficiently convert large CSV files stored in AWS S3 into the highly optimized Parquet format. <br>

Conversion method choice depends heavily on **file size** and **computational environment**.<br>

| Method | Best For | Environment | Key Feature |
| :--- | :--- | :--- | :--- |
| **Pandas / PyArrow** | Small/Medium (up to a few GB) | Single powerful instance (e.g., EC2) | Fastest I/O for in-memory data. |
| **Dask** | Large (tens of GB, > RAM) | Single instance (powerful, high RAM) | Uses **out-of-core** processing (chunks) to handle files larger than memory. |
| **PySpark / AWS Glue** | Massive (hundreds of GBs to TBs) | Distributed Cluster (AWS EMR, Databricks) | Handles true distributed computing and horizontal scaling. |


**1. Medium Scale: Pandas/PyArrow (In-Memory)<br>**
`pip install pandas s3fs pyarrow`<br>
This method is ideal when the entire CSV file fits comfortably in your machine's RAM. (Suitable for Single Machine)<br>

If your file is large (e.g., up to a few GBs) but small enough to fit within the memory limits of a single powerful machine (like a high-end EC2 instance), you can use the Pandas ecosystem enhanced by PyArrow.<br>

PyArrow is the underlying engine for Parquet files and is much faster than traditional Pandas I/O. For files slightly larger than memory, you can use an intermediate approach called Dask which utilizes PyArrow for chunked processing.<br>


**2. Large Scale: Out-of-Core Processing (Single Node)<br>**
These methods efficiently utilize a single powerful machine's CPU cores and disk I/O, streaming the data without loading the full dataset into RAM at once.<br>

**2.1 Dask (Out-of-Core Processing & Parallelism)<br>**
`pip install dask[dataframe] s3fs pyarrow`<br>
Dask breaks the large file into many smaller pandas DataFrames (partitions), processes them in parallel across your available CPU cores, and manages the execution graph. This allows for processing files much larger than RAM.<br>

**2.2 DuckDB (Streaming Conversion via SQL)** <br>
`pip install duckdb s3fs pyarrow`<br>
DuckDB is a high-performance in-process analytical data management system. By using its SQL interface, we can instruct DuckDB to read the CSV file from S3 and write the Parquet file directly.<br>

DuckDB excels here because it performs the conversion using a streaming process. It does not materialize the entire intermediate result into a single DataFrame in Python's memory. Instead, it processes chunks of data efficiently and streams them out, offering high performance with minimal memory overhead.<br>

**2.3 Polars (Lazy Evaluation & Multithreaded Optimization)<br>**

`pip install polars s3fs` <br>

Polars is a blazingly fast DataFrame library written in Rust, which offers exceptional performance on modern hardware. It uses a Lazy Evaluation strategy via pl.scan_csv() which is crucial for large files.<br>

When processing large files, Polars:<br>

- Lazy Evaluation: It does not read the file until explicitly asked to do so (.collect() or .write_parquet()). It builds an optimized execution plan first.<br>

- Streaming: It reads the large file in chunks and pipelines the computation (filtering, selecting) directly to the output, effectively streaming the result without loading the full dataset into RAM.<br>

- Multithreaded: Operations are automatically parallelized across all available CPU cores, maximizing single-node performance for column-oriented operations.<br>


**3. Massive Scale: PySpark / AWS Glue (Distributed Computing)<br>**
For true petabyte-scale data, a horizontally scalable cluster framework is required.<br>

<t>**3.1 PySpark (AWS EMR / Databricks)<br>**
For files that are hundreds of GBs or TBs, running this conversion on a cluster like AWS EMR or Databricks using PySpark is the most robust method. PySpark handles parallelism and memory management automatically.<br>

<t>**3.2 AWS Glue (Serverless ETL)<br>**
AWS Glue abstracts much of the Spark setup, providing a serverless platform for this conversion. It is the preferred method when managing a cluster is undesired.<br>
If you prefer a fully serverless solution and are already using AWS, AWS Glue (which uses Spark internally) is designed for exactly this kind of ETL (Extract, Transform, Load) job. You simply define the source and target in a Glue Studio job or a Python Shell script.<br>
