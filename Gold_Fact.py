# Databricks notebook source
df = spark.read.table("master_catalog.silver.orders")
display(df)

# COMMAND ----------

cust_df = spark.sql("select customer_id,DimCustomerKey from master_catalog.gold.dimcustomers")
prod_df = spark.sql("select product_id from master_catalog.gold.dimproducts")
display(prod_df)

# COMMAND ----------

df_fact = df.join(cust_df,df.customer_id==cust_df.customer_id, "left")\
    .join(prod_df,df.product_id==prod_df.product_id, "left")\
        .select(cust_df.DimCustomerKey, df.order_id, df.product_id, df.order_date, df.year, df.quantity, df.total_amount)
display(df_fact)

# COMMAND ----------

from delta.tables import DeltaTable

df_fact = df_fact.dropDuplicates()
if spark.catalog.tableExists("master_catalog.gold.FactOrders"):
  deltaTable = DeltaTable.forPath(spark, "abfss://gold@azurestoragee2e.dfs.core.windows.net/FactOrders")
  deltaTable.alias('trg').merge(df_fact.alias('src'), "trg.Order_id = src.Order_id AND trg.DimCustomerKey = src.DimCustomerKey\
                                AND trg.product_id = src.product_id") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()
  print("Upsert process got successfully completed")
else:
  df_fact.write.format("delta")\
    .mode('overwrite')\
    .option('path','abfss://gold@azurestoragee2e.dfs.core.windows.net/FactOrders')\
    .saveAsTable("master_catalog.gold.FactOrders")
  print("Since table not exist, new ext table got created")
  

# COMMAND ----------

display(spark.read.table("master_catalog.gold.FactOrders"))