# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the data

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@azurestoragee2e.dfs.core.windows.net/Orders")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Rescued data

# COMMAND ----------

df = df.withColumnRenamed("_rescued_data", "rescued_data")

# COMMAND ----------

count_check = df.filter(col("rescued_data").isNotNull()).count()
if count_check == 0:
    df = df.drop("rescued_data")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Data timestamp

# COMMAND ----------

df = df.withColumn("order_date", to_timestamp(col("order_date")))
df = df.withColumn("year", year(col("order_date")))
display(df)

# COMMAND ----------

window_query = Window.partitionBy("year").orderBy(col("total_amount").desc())
df = df.withColumn("RankOverYear", dense_rank().over(window_query))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using OOP's to call the function

# COMMAND ----------

class Window_function:
    def row_number(self, dataset):
        df_row = dataset.withColumn("row_number", row_number().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
        return df_row
    def rank(self, dataset):
        df_rank = dataset.withColumn("rank", rank().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))
        return df_rank
    def dense_rank(self, dataset):
        df_dense = dataset.withColumn("dense_rank", dense_rank().over(Window.partitionBy("year").orderBy(col("total_amount").desc())))        
        return df_dense


# COMMAND ----------

obj = Window_function()


# COMMAND ----------

df_test = obj.row_number(df)
df_test = obj.rank(df_test)
df_test = obj.dense_rank(df_test)
display(df_test)

# COMMAND ----------

df.write.format('delta')\
    .mode("append")\
    .save("abfss://silver@azurestoragee2e.dfs.core.windows.net/Orders")
    

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists master_catalog.silver.Orders
# MAGIC using delta
# MAGIC location "abfss://silver@azurestoragee2e.dfs.core.windows.net/Orders"