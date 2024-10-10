# Snapshot DB

This is a rewrite of the SCDM Snapshot Program Package in pure DuckDB SQL. DuckDB is a lightweight serverless in-process analytical Database.

To use this package, you need to have DuckDB installed on your machine. You can download DuckDB from [here](https://duckdb.org/). You also need a Sentinel Common Data Model (SCDM) database in parquet format available. [Parquet](https://parquet.apache.org/) is a open source columnar storage format available maintained by Apache, designed for efficient storage and retrieval of data.

The script currently outputs tables to parquet format.

## Setup

After downloading scdm_snapspot_db.sql, configure the file in the following ways:

Set the memory limit to a value that is appropriate for your machine. The default value is 96GB. You may also want to set the DPMaxDate variable to a date that is appropriate for your data. The default value is 2023-06-30. Consider [fine-tuning](https://duckdb.org/docs/guides/performance/how_to_tune_workloads) other values to suit your needs.

```
SET memory_limit = '96GB';
SET VARIABLE DPMaxDate = DATE '2023-06-30';
```

Set the paths to the SCDM database and the output directory. This step is currently manual for each table in the database. The paths are set in the following way throughout the file.

```
SELECT * FROM '/apps/socprojects/dmqa/etl25-parquet/enrollment-snappy.parquet'
```

## Usage

To use the package, run the following command in the shell:

```
/path/to/duckdb dplocal.db < scdm_snapshot_db.sql
```