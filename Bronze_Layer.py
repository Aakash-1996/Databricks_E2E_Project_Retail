# Databricks notebook source
from pyspark.sql.functions import *

dbutils.widgets.text('data', '')
data = dbutils.widgets.get('data')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read the data

# COMMAND ----------

display(spark.read.format('parquet')\
    .load(f"abfss://source@azurestoragee2e.dfs.core.windows.net/{data}"))

# COMMAND ----------

# dbutils.fs.ls(f"abfss://bronze@azurestoragee2e.dfs.core.windows.net/{data}")

# COMMAND ----------

df = spark.readStream.format('cloudFiles')\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", f"abfss://bronze@azurestoragee2e.dfs.core.windows.net/{data}/checkpoint_{data}")\
    .load(f"abfss://source@azurestoragee2e.dfs.core.windows.net/{data}")\
    .withColumn("Received_ts", current_timestamp())

display(df)    

# COMMAND ----------

df.writeStream.format('parquet')\
    .outputMode('append')\
    .option("checkpointLocation", f"abfss://bronze@azurestoragee2e.dfs.core.windows.net/{data}/checkpoint_{data}")\
    .option('path', f"abfss://bronze@azurestoragee2e.dfs.core.windows.net/{data}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

display(spark.read.format('parquet')\
    .load(f"abfss://bronze@azurestoragee2e.dfs.core.windows.net/{data}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # dbutils.fs.ls("abfss://bronze@azurestoragee2e.dfs.core.windows.net/Products")
# MAGIC dbutils.fs.ls("abfss://Silver@azurestoragee2e.dfs.core.windows.net/Products")