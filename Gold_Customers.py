# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import sha2, concat_ws

# COMMAND ----------

dbutils.widgets.text("init_load_flag", "0")
load_flag = dbutils.widgets.get("init_load_flag")

# COMMAND ----------

df = spark.sql("select * from master_catalog.silver.customers")
display(df)

# COMMAND ----------

df = df.dropDuplicates()
display(df)

# COMMAND ----------

df.createOrReplaceTempView("customer_df")

# COMMAND ----------

file_count = len(dbutils.fs.ls("abfss://gold@azurestoragee2e.dfs.core.windows.net/Customers"))
file_count

# COMMAND ----------

if file_count > 0:
  validate_init = spark.sql(''' select count(*) cnt from master_catalog.gold.DimCustomers Dim left join customer_df main
  on main.customer_id = Dim.customer_id where Dim.customer_id is null ''').first()["cnt"]
  print(validate_init)
else:
  print("No file exist in the ext location. Command skipped")

# COMMAND ----------

if file_count > 0:
    df_new = spark.sql(''' select main.customer_id customer_id, DimCustomerKey, created_date, updated_date from master_catalog.gold.DimCustomers Dim full join customer_df main
    on main.customer_id = Dim.customer_id where Dim.customer_id is null ''')

    df_old = spark.sql(''' select main.customer_id customer_id, DimCustomerKey, created_date, updated_date from master_catalog.gold.DimCustomers Dim full join customer_df main
    on main.customer_id = Dim.customer_id where Dim.customer_id is not null ''')

    df_old.createOrReplaceTempView("df_old")
    df_new.createOrReplaceTempView("df_new")
else:
    print("No file exist in the ext location. Command skipped")


# COMMAND ----------

if file_count > 0:
    max_key = spark.sql(''' select max(DimCustomerKey) max from master_catalog.gold.DimCustomers ''').first()["max"]
    print(max_key)
else:
    print("No file exist in the ext location. Command skipped")


# COMMAND ----------

if (int(load_flag) == 0) and (validate_init==0):
    df_new_full = spark.sql(f''' 
              select main.*, monotonically_increasing_id()+1+{max_key} as DimCustomerKey, current_timestamp() as created_date, current_timestamp() as updated_date from df_new left join customer_df main on main.customer_id = df_new.customer_id ''' )
    df_new_full = df_new_full.dropDuplicates()

    print(f"Incr load of {df_new_full.count()} - New records inserted")
    
    src_hashed = (
            spark.sql(""" SELECT main.*, df_old.DimCustomerKey, df_old.created_date FROM df_old
        LEFT JOIN customer_df main ON main.customer_id = df_old.customer_id """)\
        .withColumn("updated_date", current_timestamp()).dropDuplicates()\
        .withColumn("src_hash", sha2(
            concat_ws(
                "||", "customer_id", "email", "city", "state", "domain", "Full_Name", "DimCustomerKey" ), 256 ) ))

    gold_df = spark.table("master_catalog.gold.DimCustomers")
    tgt_hashed = gold_df.withColumn(
    "tgt_hash",
    sha2(concat_ws("||", "customer_id", "email", "city", "state", "domain", "Full_Name", "DimCustomerKey"), 256)
    )

    df_updated = (
        src_hashed.alias("src")
        .join(tgt_hashed.alias("tgt"), "customer_id")
        .filter(col("src.src_hash") != col("tgt.tgt_hash"))\
            .select("src.customer_id", "src.email", "src.city", "src.state", "src.domain", "src.Full_Name", "src.DimCustomerKey", "tgt.created_date", "src.updated_date")
    )

    print(f"Incr load of {df_updated.count()} - Old records inserted")

    df_full = df_new_full.unionByName(df_updated)
    
else:
    df.withColumn("DimCustomerKey", monotonically_increasing_id() + 1)\
        .withColumn("created_date", current_timestamp())\
        .withColumn("updated_date", current_timestamp())\
        .dropDuplicates()\
        .write.format('delta')\
                .mode('overwrite')\
                .option("overwriteSchema", "true")\
                .option('path','abfss://gold@azurestoragee2e.dfs.core.windows.net/Customers')\
                .saveAsTable('master_catalog.gold.DimCustomers')

    print(f"Initial load of {spark.sql("select count(*) from master_catalog.gold.DimCustomers").first()[0]} - New records inserted")
    
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from master_catalog.gold.DimCustomers 
# MAGIC -- describe history master_catalog.gold.DimCustomers
# MAGIC
# MAGIC -- restore table master_catalog.gold.DimCustomers to version as of 0

# COMMAND ----------

from delta.tables import DeltaTable

if int(load_flag) ==1: 
  print("Initial load process got successfully completed")
else:
  df_full = df_full.dropDuplicates()
  if spark.catalog.tableExists("master_catalog.gold.DimCustomers"):
    deltaTable = DeltaTable.forPath(spark, "abfss://gold@azurestoragee2e.dfs.core.windows.net/Customers")
    deltaTable.alias('trg').merge(df_full.alias('src'), 'trg.DimCustomerKey = src.DimCustomerKey') \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    print("Upsert process got successfully completed")
  else:
    df_full.write.format("delta")\
      .mode('overwrite')\
      .option('path','abfss://gold@azurestoragee2e.dfs.core.windows.net/Customers')\
      .save("master_catalog.gold.DimCustomers")
    print("Since table not exist, new ext table got created")
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select created_date, updated_date, count(DimCustomerKey) from master_catalog.gold.DimCustomers group by created_date, updated_date
# MAGIC -- truncate table master_catalog.gold.DimCustomers

# COMMAND ----------

# MAGIC %sql
# MAGIC select created_date, updated_date, count(DimCustomerKey) from master_catalog.gold.DimCustomers group by created_date, updated_date