# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Silver Layer Transformation Overview
# MAGIC %md
# MAGIC # Silver Layer Transformation
# MAGIC
# MAGIC This notebook transforms bronze tables into silver tables with data quality improvements:
# MAGIC
# MAGIC **Bronze Tables:**
# MAGIC - `retail.bronze.bronze_customers`
# MAGIC - `retail.bronze.bronze_products`
# MAGIC - `retail.bronze.bronze_orders`
# MAGIC - `retail.bronze.bronze_transactions`
# MAGIC
# MAGIC **Transformations Applied:**
# MAGIC - Remove duplicates based on primary keys
# MAGIC - Drop rows with null primary keys
# MAGIC - Standardize string formats (trim, lowercase emails)
# MAGIC - Standardize date/timestamp formats
# MAGIC - Handle null values appropriately
# MAGIC - Add data quality validation flags
# MAGIC - Preserve existing IDs from bronze layer
# MAGIC
# MAGIC **Output:** Delta tables in `retail.silver` schema

# COMMAND ----------

# DBTITLE 1,Silver Customers Transformation
# MAGIC %md
# MAGIC ## Silver Customers Transformation
# MAGIC
# MAGIC **Primary Key:** `customer_id`
# MAGIC
# MAGIC **Data Quality Steps:**
# MAGIC - Remove duplicate customer_id records (keep first occurrence)
# MAGIC - Drop rows where customer_id is null
# MAGIC - Trim all string fields
# MAGIC - Lowercase email addresses
# MAGIC - Standardize date formats for date_of_birth and registration_date
# MAGIC - Add is_valid flag for email validation
# MAGIC - Handle null values in optional fields

# COMMAND ----------

# DBTITLE 1,Transform bronze_customers to silver_customers
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read bronze customers
bronze_customers = spark.table("retail.bronze.bronze_customers")

# Apply transformations
silver_customers = (
    bronze_customers
    # Remove rows with null customer_id AND validate customer_id format (CUST_XXX)
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("customer_id").rlike(r"^CUST_\d+$"))  # Only valid customer IDs
    # Remove duplicates based on customer_id (keep first)
    .dropDuplicates(["customer_id"])
    # Trim string fields
    .withColumn("first_name", F.trim(F.col("first_name")))
    .withColumn("last_name", F.trim(F.col("last_name")))
    .withColumn("email", F.lower(F.trim(F.col("email"))))  # Lowercase email
    .withColumn("address", F.trim(F.col("address")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("country", F.trim(F.col("country")))
    .withColumn("gender", F.upper(F.trim(F.col("gender"))))  # Standardize to uppercase
    .withColumn("loyalty_tier", F.trim(F.col("loyalty_tier")))
    # Add data quality flag for email validation
    .withColumn("is_valid_email", 
                F.when(F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"), True)
                .otherwise(False))
    # Add processing timestamp
    .withColumn("processed_at", F.current_timestamp())
)

# Write to silver layer
(silver_customers
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail.silver.silver_customers"))

print(f"Silver customers table created with {silver_customers.count()} records")

# COMMAND ----------

# DBTITLE 1,Silver Products Transformation
# MAGIC %md
# MAGIC ## Silver Products Transformation
# MAGIC
# MAGIC **Primary Key:** `product_id`
# MAGIC
# MAGIC **Data Quality Steps:**
# MAGIC - Remove duplicate product_id records (keep first occurrence)
# MAGIC - Drop rows where product_id is null
# MAGIC - Trim all string fields
# MAGIC - Standardize category and sub_category names
# MAGIC - Ensure numeric fields (unit_price, cost_price, stock_qty) are non-negative
# MAGIC - Calculate profit margin
# MAGIC - Add data quality flags for pricing validation
# MAGIC - Handle null values in optional fields

# COMMAND ----------

# DBTITLE 1,Transform bronze_products to silver_products
# Read bronze products
bronze_products = spark.table("retail.bronze.bronze_products")

# Apply transformations
silver_products = (
    bronze_products
    # Remove rows with null product_id
    .filter(F.col("product_id").isNotNull())
    # Remove duplicates based on product_id (keep first)
    .dropDuplicates(["product_id"])
    # Trim string fields
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("sub_category", F.trim(F.col("sub_category")))
    .withColumn("brand", F.trim(F.col("brand")))
    .withColumn("sku", F.trim(F.col("sku")))
    # Ensure numeric fields are non-negative
    .withColumn("unit_price", F.when(F.col("unit_price") < 0, 0).otherwise(F.col("unit_price")))
    .withColumn("cost_price", F.when(F.col("cost_price") < 0, 0).otherwise(F.col("cost_price")))
    .withColumn("stock_qty", F.when(F.col("stock_qty") < 0, 0).otherwise(F.col("stock_qty")))
    # Calculate profit margin
    .withColumn("profit_margin", 
                F.when((F.col("unit_price") > 0) & (F.col("cost_price").isNotNull()),
                       ((F.col("unit_price") - F.col("cost_price")) / F.col("unit_price")) * 100)
                .otherwise(None))
    # Add data quality flags
    .withColumn("is_valid_pricing", 
                F.when((F.col("unit_price") > 0) & (F.col("cost_price") > 0) & (F.col("unit_price") >= F.col("cost_price")), True)
                .otherwise(False))
    .withColumn("is_in_stock", F.when(F.col("stock_qty") > 0, True).otherwise(False))
    # Add processing timestamp
    .withColumn("processed_at", F.current_timestamp())
)

# Write to silver layer
(silver_products
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail.silver.silver_products"))

print(f"Silver products table created with {silver_products.count()} records")

# COMMAND ----------

# DBTITLE 1,Silver Orders Transformation
# MAGIC %md
# MAGIC ## Silver Orders Transformation
# MAGIC
# MAGIC **Primary Key:** `order_id`
# MAGIC
# MAGIC **Data Quality Steps:**
# MAGIC - Remove duplicate order_id records (keep first occurrence)
# MAGIC - Drop rows where order_id is null
# MAGIC - Trim string fields (status, payment_method)
# MAGIC - Standardize order status values
# MAGIC - Ensure numeric fields are non-negative
# MAGIC - Validate line_total calculation (quantity * unit_price * (1 - discount_pct))
# MAGIC - Add data quality flags for validation
# MAGIC - Preserve foreign keys (customer_id, product_id, transaction_id)

# COMMAND ----------

# DBTITLE 1,Transform bronze_orders to silver_orders
# Read bronze orders
bronze_orders = spark.table("retail.bronze.bronze_orders")

# Apply transformations
silver_orders = (
    bronze_orders
    # Remove rows with null order_id
    .filter(F.col("order_id").isNotNull())
    # Remove duplicates based on order_id (keep first)
    .dropDuplicates(["order_id"])
    # Trim string fields
    .withColumn("status", F.upper(F.trim(F.col("status"))))  # Standardize status to uppercase
    .withColumn("payment_method", F.trim(F.col("payment_method")))
    # Ensure numeric fields are non-negative
    .withColumn("quantity", F.when(F.col("quantity") < 0, 0).otherwise(F.col("quantity")))
    .withColumn("unit_price", F.when(F.col("unit_price") < 0, 0).otherwise(F.col("unit_price")))
    .withColumn("discount_pct", F.when(F.col("discount_pct") < 0, 0)
                                 .when(F.col("discount_pct") > 1, 1)
                                 .otherwise(F.col("discount_pct")))
    .withColumn("shipping_cost", F.when(F.col("shipping_cost") < 0, 0).otherwise(F.col("shipping_cost")))
    # Calculate expected line total for validation
    .withColumn("calculated_line_total", 
                F.col("quantity") * F.col("unit_price") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0))))
    # Add data quality flags
    .withColumn("is_valid_line_total", 
                F.when(F.abs(F.col("line_total") - F.col("calculated_line_total")) < 0.01, True)
                .otherwise(False))
    .withColumn("has_discount", F.when(F.col("discount_pct") > 0, True).otherwise(False))
    .withColumn("is_returned", F.when(F.col("return_flag") == True, True).otherwise(False))
    # Extract order date from order_ts
    .withColumn("order_date", F.to_date(F.col("order_ts")))
    # Add processing timestamp
    .withColumn("processed_at", F.current_timestamp())
)

# Write to silver layer
(silver_orders
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail.silver.silver_orders"))

print(f"Silver orders table created with {silver_orders.count()} records")

# COMMAND ----------

# DBTITLE 1,Silver Transactions Transformation
# MAGIC %md
# MAGIC ## Silver Transactions Transformation
# MAGIC
# MAGIC **Primary Key:** `transaction_id`
# MAGIC
# MAGIC **Data Quality Steps:**
# MAGIC - Remove duplicate transaction_id records (keep first occurrence)
# MAGIC - Drop rows where transaction_id is null
# MAGIC - Ensure numeric fields are non-negative
# MAGIC - Validate line_total calculation (quantity * unit_price * (1 - discount_pct))
# MAGIC - Standardize transaction_date format
# MAGIC - Add data quality flags for validation
# MAGIC - Preserve foreign keys (order_id, product_id)

# COMMAND ----------

# DBTITLE 1,Transform bronze_transactions to silver_transactions
# Read bronze transactions
bronze_transactions = spark.table("retail.bronze.bronze_transactions")

# Apply transformations
silver_transactions = (
    bronze_transactions
    # Remove rows with null transaction_id
    .filter(F.col("transaction_id").isNotNull())
    # Remove duplicates based on transaction_id (keep first)
    .dropDuplicates(["transaction_id"])
    # Ensure numeric fields are non-negative
    .withColumn("quantity", F.when(F.col("quantity") < 0, 0).otherwise(F.col("quantity")))
    .withColumn("unit_price", F.when(F.col("unit_price") < 0, 0).otherwise(F.col("unit_price")))
    .withColumn("discount_pct", F.when(F.col("discount_pct") < 0, 0)
                                 .when(F.col("discount_pct") > 1, 1)
                                 .otherwise(F.col("discount_pct")))
    # Calculate expected line total for validation
    .withColumn("calculated_line_total", 
                F.col("quantity") * F.col("unit_price") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0))))
    # Add data quality flags
    .withColumn("is_valid_line_total", 
                F.when(F.abs(F.col("line_total") - F.col("calculated_line_total")) < 0.01, True)
                .otherwise(False))
    .withColumn("has_discount", F.when(F.col("discount_pct") > 0, True).otherwise(False))
    .withColumn("is_returned", F.when(F.col("return_flag") == True, True).otherwise(False))
    # Add processing timestamp
    .withColumn("processed_at", F.current_timestamp())
)

# Write to silver layer
(silver_transactions
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail.silver.silver_transactions"))

print(f"Silver transactions table created with {silver_transactions.count()} records")

# COMMAND ----------

# DBTITLE 1,Summary and Validation
# MAGIC %md
# MAGIC ## Summary and Validation
# MAGIC
# MAGIC Verify all silver tables were created successfully and review record counts and data quality metrics.

# COMMAND ----------

# DBTITLE 1,Validate silver tables
# Verify all silver tables exist and show summary statistics
tables = [
    "retail.silver.silver_customers",
    "retail.silver.silver_products",
    "retail.silver.silver_orders",
    "retail.silver.silver_transactions"
]

print("Silver Layer Summary")
print("=" * 80)

for table_name in tables:
    df = spark.table(table_name)
    count = df.count()
    print(f"\n{table_name}:")
    print(f"  Total Records: {count:,}")
    
    # Show data quality metrics if available
    if "is_valid_email" in df.columns:
        valid_emails = df.filter(F.col("is_valid_email") == True).count()
        print(f"  Valid Emails: {valid_emails:,} ({valid_emails/count*100:.1f}%)")
    
    if "is_valid_pricing" in df.columns:
        valid_pricing = df.filter(F.col("is_valid_pricing") == True).count()
        print(f"  Valid Pricing: {valid_pricing:,} ({valid_pricing/count*100:.1f}%)")
    
    if "is_valid_line_total" in df.columns:
        valid_totals = df.filter(F.col("is_valid_line_total") == True).count()
        print(f"  Valid Line Totals: {valid_totals:,} ({valid_totals/count*100:.1f}%)")
    
    if "is_returned" in df.columns:
        returned = df.filter(F.col("is_returned") == True).count()
        print(f"  Returned Items: {returned:,} ({returned/count*100:.1f}%)")

print("\n" + "=" * 80)
print("Silver layer build complete!")

# COMMAND ----------

# DBTITLE 1,View Silver Tables Data
# MAGIC %md
# MAGIC ## View Silver Tables Data
# MAGIC
# MAGIC Sample data from all silver tables with their new data quality columns.

# COMMAND ----------

# DBTITLE 1,View silver_customers sample
# MAGIC %sql
# MAGIC SELECT * FROM retail.silver.silver_customers LIMIT 10

# COMMAND ----------

# DBTITLE 1,View silver_products sample
# MAGIC %sql
# MAGIC SELECT * FROM retail.silver.silver_products LIMIT 10

# COMMAND ----------

# DBTITLE 1,View silver_orders sample
# MAGIC %sql
# MAGIC SELECT * FROM retail.silver.silver_orders LIMIT 10

# COMMAND ----------

# DBTITLE 1,View silver_transactions sample
# MAGIC %sql
# MAGIC SELECT * FROM retail.silver.silver_transactions LIMIT 10