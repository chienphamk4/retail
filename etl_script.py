# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://project1@pocdatalake0402.blob.core.windows.net",
  mount_point = "/mnt/project1",
  extra_configs = {"fs.azure.account.key.pocdatalake0402.blob.core.windows.net": "3KJ2KURMZ/tLnhfzrgARymfLbNuQfxXVb01oKoI7rh0tXVturTPPhCJhsYfRfAMawVpSfpp2pJmP+AStLeLrcA=="}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/project1/Bronze/transaction/')

# COMMAND ----------

# DBTITLE 1,Bronze Layer
df_transactions = spark.read.parquet('/mnt/project1/Bronze/transaction/')
df_products = spark.read.parquet('/mnt/project1/Bronze/product/')
df_stores = spark.read.parquet('/mnt/project1/Bronze/store/')
df_customers = spark.read.parquet('/mnt/project1/Bronze/customer')

# COMMAND ----------

display(df_stores)

# COMMAND ----------

# DBTITLE 1,Silver Layer
from pyspark.sql.functions import col


df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customers.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])

# COMMAND ----------

df_silver = df_transactions \
    .join(df_customers, "customer_id") \
    .join(df_products, "product_id") \
    .join(df_stores, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))

# COMMAND ----------


display(df_silver)

# COMMAND ----------

spark.sql("USE CATALOG hive_metastore")


# COMMAND ----------

silver_path = "/mnt/project1/silver/"

df_silver.write.mode("overwrite").format("delta").save(silver_path)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from retail_silver_cleaned
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("/mnt/project1/silver/"))


# COMMAND ----------

silver_df = spark.read.format("delta").load("/mnt/project1/silver/")

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, avg

gold_df = silver_df.groupBy(
    "transaction_date",
    "product_id", "product_name", "category",
    "store_id", "store_name", "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)


# COMMAND ----------

display(gold_df)

# COMMAND ----------

gold_path = "/mnt/retail_project/gold/"

gold_df.write.mode("overwrite").format("delta").save(gold_path)

# COMMAND ----------

gold_df.count()



# COMMAND ----------

gold_path = "/mnt/project1/gold/retail_gold_sales_summary"

gold_df.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(gold_path)




# COMMAND ----------

display(dbutils.fs.ls("/mnt/project1/gold/retail_gold_sales_summary"))


# COMMAND ----------

spark.sql("SELECT current_catalog()").show()



# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS default.retail_gold_sales_summary
USING DELTA
LOCATION 'dbfs:/mnt/project1/gold/retail_gold_sales_summary'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.retail_gold_sales_summary
# MAGIC

# COMMAND ----------

