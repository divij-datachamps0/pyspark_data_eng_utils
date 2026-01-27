# pyspark_data_utils

A collection of utility functions and classes for PySpark-based data engineering workflows, designed for use in Databricks notebooks and Azure Data Lake Storage (ADLS) environments.

## Features

- **ADLS Connector**: Easily configure Spark to connect to Azure Data Lake Storage Gen2 using OAuth credentials.
- **Notebook Execution Timer**: Track and summarize execution time for notebook sections and overall runtime.
- **Notebook Logger**: Lightweight CSV logger for Databricks notebooks, with support for IST timestamps and ADLS storage.
- **Single CSV Writer**: Write Spark DataFrames as single CSV files to ADLS, with optional timestamped filenames.
- **Safe CSV Reader**: Robust CSV reading with error handling, schema validation, and logging.
- **DataFrame Utilities**:
  - Average non-zero values in a column
  - Get DataFrame shape (rows, columns)
  - Check if a column exists
  - Compare distinct cardinality between DataFrames
  - Standardize column names to lower_snake_case
- **Event Logging**: Log ETL job events to a central Delta table for pipeline monitoring.

## Usage

### Importing Utilities
Use `%run /Shared/utils` at the start of your Databricks notebook to inherit all UDFs and utilities. No need to re-import libraries already present in the notebook.

### Example: Notebook Timer
```python
start_notebook()
start_block("Load Data")
# ... your code ...
end_block()
end_notebook()
```

### Example: ADLS Connector
```python
adls_path = adls_connector(accountName, fileSystemName, idKey, secretKey, tenant_id)
```

### Example: Write Single CSV
```python
write_single_csv(df, base_path, "output_filename")
```

### Example: Safe CSV Read
```python
df = safe_read_csv(path, expected_cols=["col1", "col2"])
```

## Requirements
- PySpark
- Databricks environment (for dbutils)
- Azure Data Lake Storage Gen2 (for ADLS functions)
- pandas
- inflection (for column standardization)

## Notes
- Some parameters (file paths, secret keys) are workspace-specific. Check before using in another workspace.
- Logging and file operations assume Databricks and ADLS mounts are available.
- For IST timezone support, either `zoneinfo` (Python 3.9+) or `pytz` is used.

## Author
Divij Kulshrestha

## License
MIT License
