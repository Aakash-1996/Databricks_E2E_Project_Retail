This project focuses on data engineering in a retail domain, involving Customers, Products, Regions, and Orders.

A data pipeline was created to ingest source files from Azure Blob Storage, transform the data, and load the refined data back into storage.

The entire pipeline follows the Medallion Architecture.

PySpark and SQL were used for data ingestion in the Bronze layer.
Notebooks were used for data transformation in the Silver layer.

Delta Live Tables (DLT) and notebooks were used to implement Slowly Changing Dimensions (SCD) both manually and automatically.
Finally, a STAR schema model was created to load the curated data into the Gold layer.
