# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,List contents of retail raw volume
display(dbutils.fs.ls("/Volumes/retail/raw/raw_layer"))

# COMMAND ----------

# DBTITLE 1,Check customers.csv schema
# Preview customers data with proper multi-line handling
customers_df = spark.read.csv(
    "/Volumes/retail/raw/raw_layer/customers.csv", 
    header=True, 
    inferSchema=True,
    multiLine=True,  # Handle fields with embedded newlines
    escape='"'       # Handle quoted fields properly
)
print(f"Customers - Rows: {customers_df.count()}")
customers_df.printSchema()
display(customers_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Check orders_part-0000.csv schema
# Preview orders data
orders_df = spark.read.csv("/Volumes/retail/raw/raw_layer/orders_part-0000.csv", header=True, inferSchema=True)
print(f"Orders - Rows: {orders_df.count()}")
orders_df.printSchema()
display(orders_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Check products.csv schema
# Preview products data
products_df = spark.read.csv("/Volumes/retail/raw/raw_layer/products.csv", header=True, inferSchema=True)
print(f"Products - Rows: {products_df.count()}")
products_df.printSchema()
display(products_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Check transactions.csv schema
# Preview transactions data
transactions_df = spark.read.csv("/Volumes/retail/raw/raw_layer/transactions.csv", header=True, inferSchema=True)
print(f"Transactions - Rows: {transactions_df.count()}")
transactions_df.printSchema()
display(transactions_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Write customers to bronze
customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail.bronze.bronze_customers")

# COMMAND ----------

# DBTITLE 1,Write products to bronze
products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail.bronze.bronze_products")

# COMMAND ----------

# DBTITLE 1,Write orders to bronze
orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail.bronze.bronze_orders")

# COMMAND ----------

# DBTITLE 1,Write transactions to bronze
transactions_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail.bronze.bronze_transactions")

# COMMAND ----------

bronze_customers = spark.table("retail.bronze.bronze_customers")