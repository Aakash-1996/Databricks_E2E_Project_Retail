# Databricks notebook source
df = spark.read.table("master_catalog.bronze.region")
display(df)

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
    .save("abfss://silver@azurestoragee2e.dfs.core.windows.net/Region")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a external delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists master_catalog.silver.Region
# MAGIC using delta
# MAGIC location "abfss://silver@azurestoragee2e.dfs.core.windows.net/Region"

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`abfss://bronze@azurestoragee2e.dfs.core.windows.net/Customers_test`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use master_catalog.bronze;
# MAGIC select * from Customers_test

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL master_catalog.bronze.customers_test;