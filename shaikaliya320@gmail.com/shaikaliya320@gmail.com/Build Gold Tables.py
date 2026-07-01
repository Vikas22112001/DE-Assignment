# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer Overview
# MAGIC %md
# MAGIC # Gold Layer - Business KPIs & Aggregations
# MAGIC
# MAGIC This notebook creates gold tables with business-level metrics and aggregations from the silver layer.
# MAGIC
# MAGIC **Source Tables:**
# MAGIC - `retail.silver.silver_customers` (100 rows)
# MAGIC - `retail.silver.silver_products` (100 rows)
# MAGIC - `retail.silver.silver_transactions` (1,518 rows) - Primary fact table
# MAGIC - `retail.silver.silver_orders` (2 rows)
# MAGIC
# MAGIC **Gold Tables to Build:**
# MAGIC 1. **gold_total_revenue** - Revenue aggregations by date, month, year
# MAGIC 2. **gold_top_customers** - Customer spend analysis and rankings
# MAGIC 3. **gold_top_products** - Product performance metrics
# MAGIC 4. **gold_avg_order_value** - Average order value trends
# MAGIC 5. **gold_return_rate** - Return rate analysis by product and category
# MAGIC
# MAGIC **Output:** Delta tables in `retail.gold` schema

# COMMAND ----------

# DBTITLE 1,1. Gold Total Revenue
# MAGIC %md
# MAGIC ## 1. Gold Total Revenue
# MAGIC
# MAGIC **Business Metrics:**
# MAGIC - Total revenue by day, month, and year
# MAGIC - Transaction count and average transaction value
# MAGIC - Cumulative revenue tracking
# MAGIC
# MAGIC **Granularity:** Daily aggregations with derived month/year columns

# COMMAND ----------

# DBTITLE 1,Build gold_total_revenue
from pyspark.sql import functions as F

# Read silver tables
transactions = spark.table("retail.silver.silver_transactions")
orders = spark.table("retail.silver.silver_orders")

# Join transactions with orders to get order-level data
txn_orders = transactions.join(orders.select("order_id"), "order_id", "inner")

# Calculate daily revenue metrics aligned with ERD schema
gold_total_revenue = (
    txn_orders
    .filter(F.col("is_returned") == False)  # Exclude returned transactions
    .groupBy("transaction_date")
    .agg(
        F.sum("line_total").alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.count("transaction_id").alias("total_transactions")
    )
    .select(
        F.col("transaction_date").alias("snapshot_date"),
        "total_revenue",
        "total_orders",
        "total_transactions",
        F.current_timestamp().alias("_updated_at")
    )
    .orderBy("snapshot_date")
)

# Write to gold layer
(gold_total_revenue
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail.gold.gold_total_revenue"))

print(f"✅ Gold total revenue table created with {gold_total_revenue.count()} daily records (ERD aligned)")
display(gold_total_revenue.limit(10))

# COMMAND ----------

# DBTITLE 1,2. Gold Top Customers
# MAGIC %md
# MAGIC ## 2. Gold Top Customers
# MAGIC
# MAGIC **Business Metrics:**
# MAGIC - Total spend per customer (lifetime value)
# MAGIC - Order count and average order value
# MAGIC - Customer ranking by spend
# MAGIC - Customer demographic enrichment (city, state, loyalty tier)
# MAGIC
# MAGIC **Granularity:** One row per customer with aggregated metrics

# COMMAND ----------

# DBTITLE 1,Build gold_top_customers
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read silver tables
txn = spark.table("retail.silver.silver_transactions").alias("txn")
customers = spark.table("retail.silver.silver_customers")
orders = spark.table("retail.silver.silver_orders").select("order_id", "customer_id").alias("ord")

# Aggregate customer metrics from transactions
customer_metrics = (
    txn
    .join(orders, "order_id", "inner")
    .filter(F.col("txn.is_returned") == False)  # Exclude returns
    .groupBy("customer_id")
    .agg(
        F.sum("txn.line_total").alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders")
    )
    .withColumn("avg_order_value", F.col("total_revenue") / F.col("total_orders"))
)

# Join with customer details and build ERD-aligned schema
gold_top_customers = (
    customer_metrics
    .join(customers, "customer_id", "inner")
    .select(
        "customer_id",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("full_name"),
        F.col("email").alias("contact"),
        "total_revenue",
        "total_orders",
        "avg_order_value"
    )
    # Add revenue ranking
    .withColumn("revenue_rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
    .withColumn("_updated_at", F.current_timestamp())
    .select(
        "customer_id",
        "full_name",
        "contact",
        "total_revenue",
        "total_orders",
        "avg_order_value",
        "revenue_rank",
        "_updated_at"
    )
    .orderBy("revenue_rank")
)

# Write to gold layer
(gold_top_customers
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail.gold.gold_top_customers"))

print(f"✅ Gold top customers table created with {gold_top_customers.count()} customers (ERD aligned)")
display(gold_top_customers.limit(10))

# COMMAND ----------

# DBTITLE 1,3. Gold Top Products
# MAGIC %md
# MAGIC ## 3. Gold Top Products
# MAGIC
# MAGIC **Business Metrics:**
# MAGIC - Total revenue per product
# MAGIC - Total quantity sold and transaction count
# MAGIC - Total profit (revenue - cost)
# MAGIC - Profit margin percentage
# MAGIC - Product ranking by revenue
# MAGIC
# MAGIC **Granularity:** One row per product with aggregated metrics

# COMMAND ----------

# DBTITLE 1,Build gold_top_products
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read silver tables
transactions = spark.table("retail.silver.silver_transactions")
products = spark.table("retail.silver.silver_products")

# Aggregate product metrics from transactions
product_metrics = (
    transactions
    .filter(F.col("is_returned") == False)  # Exclude returns
    .groupBy("product_id")
    .agg(
        F.sum("line_total").alias("total_revenue"),
        F.sum("quantity").alias("total_qty_sold")
    )
)

# Join with product details and build ERD-aligned schema
gold_top_products = (
    product_metrics
    .join(products, "product_id", "inner")
    .select(
        "product_id",
        "product_name",
        F.col("category").alias("category_name"),
        "total_qty_sold",
        "total_revenue"
    )
    # Add sales ranking by revenue
    .withColumn("sales_rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
    .withColumn("_updated_at", F.current_timestamp())
    .select(
        "product_id",
        "product_name",
        "category_name",
        "total_qty_sold",
        "total_revenue",
        "sales_rank",
        "_updated_at"
    )
    .orderBy("sales_rank")
)

# Write to gold layer
(gold_top_products
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail.gold.gold_top_products"))

print(f"✅ Gold top products table created with {gold_top_products.count()} products (ERD aligned)")
display(gold_top_products.limit(10))

# COMMAND ----------

# DBTITLE 1,4. Gold Average Order Value
# MAGIC %md
# MAGIC ## 4. Gold Average Order Value (AOV)
# MAGIC
# MAGIC **Business Metrics:**
# MAGIC - Average order value by day, month, year
# MAGIC - Order count and total revenue
# MAGIC - Moving averages for trend analysis
# MAGIC
# MAGIC **Granularity:** Daily aggregations with derived month/year columns

# COMMAND ----------

# DBTITLE 1,Build gold_avg_order_value
from pyspark.sql import functions as F

# Read silver tables
transactions = spark.table("retail.silver.silver_transactions")

# Calculate order-level totals first
order_totals = (
    transactions
    .filter(F.col("is_returned") == False)  # Exclude returns
    .groupBy("order_id", "transaction_date")
    .agg(
        F.sum("line_total").alias("order_total")
    )
)

# Calculate daily AOV metrics aligned with ERD schema
gold_avg_order_value = (
    order_totals
    .groupBy("transaction_date")
    .agg(
        F.sum("order_total").alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
        F.avg("order_total").alias("avg_order_value")
    )
    .select(
        F.col("transaction_date").alias("snapshot_date"),
        "total_revenue",
        "total_orders",
        "avg_order_value",
        F.current_timestamp().alias("_updated_at")
    )
    .orderBy("snapshot_date")
)

# Write to gold layer
(gold_avg_order_value
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail.gold.gold_avg_order_value"))

print(f"✅ Gold avg order value table created with {gold_avg_order_value.count()} daily records (ERD aligned)")
display(gold_avg_order_value.limit(10))

# COMMAND ----------

# DBTITLE 1,5. Gold Return Rate
# MAGIC %md
# MAGIC ## 5. Gold Return Rate
# MAGIC
# MAGIC **Business Metrics:**
# MAGIC - Return rate by product, category, and time period
# MAGIC - Count of returned vs non-returned transactions
# MAGIC - Revenue impact of returns
# MAGIC
# MAGIC **Granularity:** Multiple views - by product, by category, and by time period

# COMMAND ----------

# DBTITLE 1,Build gold_return_rate
from pyspark.sql import functions as F

# Read silver tables
transactions = spark.table("retail.silver.silver_transactions").alias("t")
orders = spark.table("retail.silver.silver_orders").alias("o")

# Join transactions with orders to get order-level return status
# Use aliases to avoid ambiguous column reference
txn_orders = (
    transactions
    .join(orders.select("order_id", "return_flag"), "order_id", "inner")
)

# Calculate daily return metrics aligned with ERD schema
# An order is considered returned if it has return_flag = True from orders table
gold_return_rate = (
    txn_orders
    .groupBy(F.col("t.transaction_date"))
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct(
            F.when(F.col("o.return_flag") == True, F.col("order_id"))
        ).alias("returned_orders")
    )
    .withColumn(
        "return_rate_pct",
        (F.col("returned_orders") / F.col("total_orders")) * 100
    )
    .select(
        F.col("transaction_date").alias("snapshot_date"),
        "total_orders",
        "returned_orders",
        "return_rate_pct",
        F.current_timestamp().alias("_updated_at")
    )
    .orderBy("snapshot_date")
)

# Write to gold layer
(gold_return_rate
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retail.gold.gold_return_rate"))

print(f"✅ Gold return rate table created with {gold_return_rate.count()} daily records (ERD aligned)")
display(gold_return_rate.limit(10))

# COMMAND ----------

# DBTITLE 1,Summary and Validation
# MAGIC %md
# MAGIC ## Summary and Validation
# MAGIC
# MAGIC Verify all gold tables were created successfully and display summary statistics.

# COMMAND ----------

# DBTITLE 1,Validate gold tables
# Verify all gold tables exist and show summary statistics
gold_tables = [
    "retail.gold.gold_total_revenue",
    "retail.gold.gold_top_customers",
    "retail.gold.gold_top_products",
    "retail.gold.gold_avg_order_value",
    "retail.gold.gold_return_rate"
]

print("Gold Layer Summary")
print("=" * 80)

for table_name in gold_tables:
    df = spark.table(table_name)
    count = df.count()
    print(f"\n{table_name}:")
    print(f"  Total Records: {count:,}")
    
    # Show relevant metrics for each table
    if "total_revenue" in table_name:
        total_rev = df.agg(F.sum("daily_revenue")).collect()[0][0]
        print(f"  Total Revenue: ${total_rev:,.2f}")
    
    elif "top_customers" in table_name:
        top_customer = df.orderBy(F.desc("total_spend")).first()
        print(f"  Top Customer: {top_customer['first_name']} {top_customer['last_name']} (${top_customer['total_spend']:,.2f})")
    
    elif "top_products" in table_name:
        top_product = df.orderBy(F.desc("total_revenue")).first()
        print(f"  Top Product: {top_product['product_name']} (${top_product['total_revenue']:,.2f})")
    
    elif "avg_order_value" in table_name:
        overall_aov = df.agg(F.avg("avg_order_value")).collect()[0][0]
        print(f"  Overall AOV: ${overall_aov:,.2f}")
    
    elif "return_rate" in table_name:
        overall_return_rate = df.agg(
            (F.sum("returned_transactions") / F.sum("total_transactions") * 100)
        ).collect()[0][0]
        print(f"  Overall Return Rate: {overall_return_rate:.2f}%")

print("\n" + "=" * 80)
print("Gold layer build complete!")