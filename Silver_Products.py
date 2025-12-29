# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@azurestoragee2e.dfs.core.windows.net/Products")

display(df)    

# COMMAND ----------

count_check = df.filter(col("_rescued_data").isNotNull()).count()
if count_check == 0:
    df = df.drop("_rescued_data")
else:
    df

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function master_catalog.bronze.discounted_price (p_value double)
# MAGIC   returns double
# MAGIC   language python
# MAGIC   as
# MAGIC     $$
# MAGIC         return p_value * 0.90
# MAGIC     $$

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, product_name, master_catalog.bronze.discounted_price(price) discounted_price from  products

# COMMAND ----------

df = df.withColumn("Discounted_Price", expr("master_catalog.bronze.discounted_price(price)"))
display(df)

# COMMAND ----------

df.write.format("delta")\
    .mode("append")\
    .save("abfss://silver@azurestoragee2e.dfs.core.windows.net/Products")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists master_catalog.silver.Products
# MAGIC using delta
# MAGIC location "abfss://silver@azurestoragee2e.dfs.core.windows.net/Products"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from master_catalog.silver.Products 
# MAGIC where product_id = 'P0497'

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()