# Databricks notebook source
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the data

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@azurestoragee2e.dfs.core.windows.net/Customers")
    
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handle Rescued data

# COMMAND ----------

count_check = df.filter(col("_rescued_data").isNotNull()).count()
if count_check == 0:
    df = df.drop("_rescued_data")
else:
    df

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

df = df.withColumn("domain", split(col("email"), "@")[1])
display(df)

# COMMAND ----------

display(
    df.groupBy("domain").agg( count( col("customer_id") ).alias("Count") ).sort(col("Count"), ascending=False)
)


# COMMAND ----------

display( df.filter(col("domain")=="gmail.com") )


# COMMAND ----------

df = df.withColumn("Full_Name", concat( col('first_name'), lit(' '), col('last_name') ))
df = df.drop('first_name', 'last_name')
display(df)

# COMMAND ----------

df.write.format('delta')\
    .mode('append')\
    .save('abfss://silver@azurestoragee2e.dfs.core.windows.net/Customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists master_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists master_catalog.silver.customers
# MAGIC using delta
# MAGIC location "abfss://silver@azurestoragee2e.dfs.core.windows.net/Customers"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from master_catalog.silver.customers