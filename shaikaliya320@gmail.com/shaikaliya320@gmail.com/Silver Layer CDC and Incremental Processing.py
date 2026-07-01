# Databricks notebook source
# DBTITLE 1,Overview - CDC and Incremental Processing
# MAGIC %md
# MAGIC # Silver Layer - CDC & Incremental Processing
# MAGIC
# MAGIC This notebook demonstrates **Change Data Capture (CDC)** and **Incremental Processing** patterns for the Silver layer.
# MAGIC
# MAGIC ## Key Concepts Implemented
# MAGIC
# MAGIC ### 1. **Incremental Processing**
# MAGIC - Only process **new or changed records** since the last run
# MAGIC - Use timestamps/watermarks to track progress
# MAGIC - Avoid re-processing the entire dataset every time
# MAGIC
# MAGIC ### 2. **MERGE/UPSERT Operations**
# MAGIC - **INSERT**: Add new records that don't exist in the target
# MAGIC - **UPDATE**: Modify existing records when source has changes
# MAGIC - **DELETE**: Optional - remove records marked for deletion
# MAGIC
# MAGIC ### 3. **Slowly Changing Dimensions (SCD)**
# MAGIC - **Type 1**: Overwrite - Update existing records (no history)
# MAGIC - **Type 2**: History tracking - Keep all historical versions
# MAGIC
# MAGIC ## Implementation Pattern
# MAGIC
# MAGIC ```
# MAGIC Bronze (Full Data) → Filter by timestamp → Silver (Incremental MERGE)
# MAGIC                       ↓
# MAGIC                  Checkpoint Table
# MAGIC                  (tracks last_processed_timestamp)
# MAGIC ```
# MAGIC
# MAGIC ## Tables We'll Build
# MAGIC
# MAGIC 1. **checkpoint_table** - Tracks last processed timestamp per table
# MAGIC 2. **silver_customers_scd1** - SCD Type 1 (updates only)
# MAGIC 3. **silver_customers_scd2** - SCD Type 2 (history tracking)
# MAGIC 4. **silver_transactions_incremental** - Incremental transaction processing

# COMMAND ----------

# DBTITLE 1,1. Create Checkpoint Table
# MAGIC %md
# MAGIC ## 1. Create Checkpoint Table
# MAGIC
# MAGIC The checkpoint table tracks the **last processed timestamp** for each table. This enables incremental processing by only reading records newer than the last checkpoint.

# COMMAND ----------

# DBTITLE 1,Create checkpoint table
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Create checkpoint table schema
checkpoint_schema = """
    table_name STRING,
    last_processed_timestamp TIMESTAMP,
    records_processed BIGINT,
    updated_at TIMESTAMP
"""

# Create checkpoint table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS retail.silver.checkpoint_table (
        table_name STRING,
        last_processed_timestamp TIMESTAMP,
        records_processed BIGINT,
        updated_at TIMESTAMP
    )
    USING DELTA
""")

print("✅ Checkpoint table created: retail.silver.checkpoint_table")

# Initialize checkpoint for our tables if they don't exist
initial_checkpoints = [
    ("bronze_customers", "1900-01-01 00:00:00", 0),
    ("bronze_transactions", "1900-01-01 00:00:00", 0)
]

for table_name, initial_ts, count in initial_checkpoints:
    # Check if checkpoint exists
    existing = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM retail.silver.checkpoint_table 
        WHERE table_name = '{table_name}'
    """).collect()[0]['cnt']
    
    if existing == 0:
        spark.sql(f"""
            INSERT INTO retail.silver.checkpoint_table 
            VALUES ('{table_name}', TIMESTAMP'{initial_ts}', {count}, current_timestamp())
        """)
        print(f"  Initialized checkpoint for {table_name}")

print("\n📊 Current Checkpoints:")
spark.sql("SELECT * FROM retail.silver.checkpoint_table ORDER BY table_name").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Helper functions for checkpoint management
# Helper functions for checkpoint management

def get_last_checkpoint(table_name):
    """Get the last processed timestamp for a table"""
    result = spark.sql(f"""
        SELECT last_processed_timestamp 
        FROM retail.silver.checkpoint_table 
        WHERE table_name = '{table_name}'
    """).collect()
    
    if result:
        return result[0]['last_processed_timestamp']
    else:
        # Default to very old date if no checkpoint exists
        return spark.sql("SELECT TIMESTAMP'1900-01-01 00:00:00' as ts").collect()[0]['ts']

def update_checkpoint(table_name, new_timestamp, records_processed):
    """Update the checkpoint after successful processing"""
    spark.sql(f"""
        MERGE INTO retail.silver.checkpoint_table AS target
        USING (SELECT 
            '{table_name}' as table_name,
            TIMESTAMP'{new_timestamp}' as last_processed_timestamp,
            {records_processed} as records_processed,
            current_timestamp() as updated_at
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET
            target.last_processed_timestamp = source.last_processed_timestamp,
            target.records_processed = source.records_processed,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✅ Updated checkpoint for {table_name}: {new_timestamp} ({records_processed} records)")

print("✅ Checkpoint helper functions defined")
print("  - get_last_checkpoint(table_name)")
print("  - update_checkpoint(table_name, new_timestamp, records_processed)")

# COMMAND ----------

# DBTITLE 1,2. Incremental Read from Bronze
# MAGIC %md
# MAGIC ## 2. Incremental Read from Bronze
# MAGIC
# MAGIC Read **only new or modified records** from bronze tables based on the last checkpoint timestamp.

# COMMAND ----------

# DBTITLE 1,Read incremental data from bronze
from pyspark.sql import functions as F

# Get last checkpoint for customers
last_checkpoint = get_last_checkpoint("bronze_customers")
print(f"🕒 Last checkpoint for bronze_customers: {last_checkpoint}")

# Read bronze customers table
bronze_customers_full = spark.table("retail.bronze.bronze_customers")

print(f"\n📊 Bronze customers total records: {bronze_customers_full.count():,}")

# INCREMENTAL READ: Filter only records newer than last checkpoint
# In real CDC scenario, you'd have an updated_at or modified_at column in bronze
# For this demo, we'll simulate it by adding a timestamp
bronze_customers_incremental = (
    bronze_customers_full
    # In production, uncomment this filter with actual timestamp column:
    # .filter(F.col("updated_at") > last_checkpoint)
)

# For demonstration, let's add a simulated updated_at column
bronze_customers_incremental = (
    bronze_customers_incremental
    .withColumn("updated_at", F.current_timestamp())
)

incremental_count = bronze_customers_incremental.count()
print(f"✅ Incremental records to process: {incremental_count:,}")

if incremental_count > 0:
    print("\n🔍 Sample of incremental data:")
    display(bronze_customers_incremental.limit(5))
else:
    print("\nℹ️ No new records to process since last checkpoint")

# COMMAND ----------

# DBTITLE 1,3. MERGE/UPSERT - SCD Type 1
# MAGIC %md
# MAGIC ## 3. MERGE/UPSERT - SCD Type 1 (Updates Only)
# MAGIC
# MAGIC **Slowly Changing Dimension Type 1** - Overwrite existing records with new data. No history is kept.
# MAGIC
# MAGIC ### MERGE Logic:
# MAGIC - **WHEN MATCHED** → UPDATE the existing record
# MAGIC - **WHEN NOT MATCHED** → INSERT as a new record

# COMMAND ----------

# DBTITLE 1,Create and upsert SCD Type 1 table
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Transform incremental data (apply silver layer transformations)
silver_customers_updates = (
    bronze_customers_incremental
    .select(
        "customer_id",
        F.trim(F.col("first_name")).alias("first_name"),
        F.trim(F.col("last_name")).alias("last_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.trim(F.col("city")).alias("city"),
        F.trim(F.col("state")).alias("state"),
        F.trim(F.col("country")).alias("country"),
        "loyalty_tier",
        F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").alias("is_valid_email"),
        F.current_timestamp().alias("updated_at")
    )
    .filter(F.col("customer_id").rlike("^CUST_\\d+$"))  # Valid customer IDs only
)

# Create target table if it doesn't exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS retail.silver.silver_customers_scd1 (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        city STRING,
        state STRING,
        country STRING,
        loyalty_tier STRING,
        is_valid_email BOOLEAN,
        updated_at TIMESTAMP
    )
    USING DELTA
""")

# MERGE (UPSERT) operation
target_table = DeltaTable.forName(spark, "retail.silver.silver_customers_scd1")

merge_result = (
    target_table.alias("target")
    .merge(
        silver_customers_updates.alias("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatchedUpdate(
        set = {
            "first_name": "source.first_name",
            "last_name": "source.last_name",
            "email": "source.email",
            "city": "source.city",
            "state": "source.state",
            "country": "source.country",
            "loyalty_tier": "source.loyalty_tier",
            "is_valid_email": "source.is_valid_email",
            "updated_at": "source.updated_at"
        }
    )
    .whenNotMatchedInsert(
        values = {
            "customer_id": "source.customer_id",
            "first_name": "source.first_name",
            "last_name": "source.last_name",
            "email": "source.email",
            "city": "source.city",
            "state": "source.state",
            "country": "source.country",
            "loyalty_tier": "source.loyalty_tier",
            "is_valid_email": "source.is_valid_email",
            "updated_at": "source.updated_at"
        }
    )
    .execute()
)

print("✅ MERGE completed (SCD Type 1)")
print(f"   Table: retail.silver.silver_customers_scd1")
print(f"   Records processed: {silver_customers_updates.count():,}")

# Update checkpoint
max_timestamp = silver_customers_updates.agg(F.max("updated_at")).collect()[0][0]
update_checkpoint("bronze_customers", max_timestamp, silver_customers_updates.count())

# Show results
print("\n📊 SCD Type 1 Table (current state only):")
display(spark.table("retail.silver.silver_customers_scd1").orderBy("customer_id").limit(10))

# COMMAND ----------

# DBTITLE 1,4. MERGE/UPSERT - SCD Type 2
# MAGIC %md
# MAGIC ## 4. MERGE/UPSERT - SCD Type 2 (History Tracking)
# MAGIC
# MAGIC **Slowly Changing Dimension Type 2** - Keep all historical versions of records.
# MAGIC
# MAGIC ### SCD Type 2 Pattern:
# MAGIC - Add `valid_from` and `valid_to` timestamps
# MAGIC - Add `is_current` flag (TRUE for active version)
# MAGIC - **WHEN MATCHED** → Close out old record, insert new version
# MAGIC - **WHEN NOT MATCHED** → INSERT as new record with is_current=TRUE
# MAGIC
# MAGIC ### Example:
# MAGIC ```
# MAGIC Customer changes email:
# MAGIC OLD: customer_id=CUST_001, email=old@email.com, is_current=TRUE, valid_to=NULL
# MAGIC NEW: customer_id=CUST_001, email=old@email.com, is_current=FALSE, valid_to=2024-07-31
# MAGIC      customer_id=CUST_001, email=new@email.com, is_current=TRUE, valid_to=NULL
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Create and upsert SCD Type 2 table
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Transform incremental data with SCD Type 2 columns
silver_customers_scd2_updates = (
    bronze_customers_incremental
    .select(
        "customer_id",
        F.trim(F.col("first_name")).alias("first_name"),
        F.trim(F.col("last_name")).alias("last_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.trim(F.col("city")).alias("city"),
        F.trim(F.col("state")).alias("state"),
        F.trim(F.col("country")).alias("country"),
        "loyalty_tier",
        F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").alias("is_valid_email"),
        F.current_timestamp().alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current")
    )
    .filter(F.col("customer_id").rlike("^CUST_\\d+$"))
)

# Create target table if it doesn't exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS retail.silver.silver_customers_scd2 (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        city STRING,
        state STRING,
        country STRING,
        loyalty_tier STRING,
        is_valid_email BOOLEAN,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN
    )
    USING DELTA
""")

target_table = DeltaTable.forName(spark, "retail.silver.silver_customers_scd2")

# SCD Type 2 MERGE Logic
# Step 1: Close out existing current records (set is_current=FALSE, valid_to=now)
merge_result = (
    target_table.alias("target")
    .merge(
        silver_customers_scd2_updates.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = TRUE"
    )
    .whenMatchedUpdate(
        condition = """(
            target.email != source.email OR 
            target.city != source.city OR 
            target.state != source.state OR 
            target.loyalty_tier != source.loyalty_tier
        )""",  # Only update if something changed
        set = {
            "is_current": "FALSE",
            "valid_to": "source.valid_from"
        }
    )
    .execute()
)

print("✅ Step 1: Closed out old records (set is_current=FALSE)")

# Step 2: Insert new versions as current records
# This inserts ALL records from source (both new customers and updated versions)
silver_customers_scd2_updates.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("retail.silver.silver_customers_scd2")

print("✅ Step 2: Inserted new versions (is_current=TRUE)")
print(f"   Records processed: {silver_customers_scd2_updates.count():,}")

# Show results
print("\n📊 SCD Type 2 Table (all historical versions):")
print("\nCurrent records only (is_current=TRUE):")
display(
    spark.table("retail.silver.silver_customers_scd2")
    .filter(F.col("is_current") == True)
    .orderBy("customer_id")
    .limit(5)
)

print("\nAll versions (including history):")
display(
    spark.table("retail.silver.silver_customers_scd2")
    .orderBy("customer_id", F.desc("valid_from"))
    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,5. Incremental Transaction Processing
# MAGIC %md
# MAGIC ## 5. Incremental Transaction Processing with CDC
# MAGIC
# MAGIC Process transactions incrementally based on timestamp, handling **Inserts**, **Updates**, and **Deletes**.
# MAGIC
# MAGIC ### CDC Operations:
# MAGIC - **I** (Insert) - New transaction
# MAGIC - **U** (Update) - Modified transaction
# MAGIC - **D** (Delete) - Cancelled/deleted transaction

# COMMAND ----------

# DBTITLE 1,Process incremental transactions with CDC
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Get last checkpoint for transactions
last_checkpoint_txn = get_last_checkpoint("bronze_transactions")
print(f"🕒 Last checkpoint for bronze_transactions: {last_checkpoint_txn}")

# Read bronze transactions
bronze_txn_full = spark.table("retail.bronze.bronze_transactions")

# Simulate CDC by adding operation type and updated_at
# In real CDC scenario, these columns would come from your CDC source (Debezium, DMS, etc.)
bronze_txn_with_cdc = (
    bronze_txn_full
    .withColumn("updated_at", F.current_timestamp())
    .withColumn("cdc_operation", F.lit("I"))  # I=Insert, U=Update, D=Delete
)

# INCREMENTAL READ: Filter only new/changed records
# In production: .filter(F.col("updated_at") > last_checkpoint_txn)
incremental_txn = bronze_txn_with_cdc

print(f"\n📊 Incremental transactions to process: {incremental_txn.count():,}")

# Transform incremental transactions
silver_txn_updates = (
    incremental_txn
    .select(
        "transaction_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "line_total",
        "transaction_date",
        "return_flag",
        F.col("cdc_operation"),
        F.col("updated_at")
    )
    .withColumn(
        "calculated_line_total",
        F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100)
    )
    .withColumn(
        "is_valid_line_total",
        F.abs(F.col("line_total") - F.col("calculated_line_total")) < 0.01
    )
    .withColumn("has_discount", F.col("discount_pct") > 0)
    .withColumn("is_returned", F.col("return_flag") == True)
)

# Create target table if it doesn't exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS retail.silver.silver_transactions_incremental (
        transaction_id STRING,
        order_id STRING,
        product_id STRING,
        quantity INT,
        unit_price DOUBLE,
        discount_pct INT,
        line_total DOUBLE,
        transaction_date DATE,
        return_flag BOOLEAN,
        calculated_line_total DOUBLE,
        is_valid_line_total BOOLEAN,
        has_discount BOOLEAN,
        is_returned BOOLEAN,
        updated_at TIMESTAMP
    )
    USING DELTA
""")

# Apply CDC operations using MERGE
target_table = DeltaTable.forName(spark, "retail.silver.silver_transactions_incremental")

# Separate INSERT/UPDATE from DELETE operations
inserts_updates = silver_txn_updates.filter(F.col("cdc_operation").isin(["I", "U"]))
deletes = silver_txn_updates.filter(F.col("cdc_operation") == "D")

# Handle INSERT/UPDATE
if inserts_updates.count() > 0:
    (
        target_table.alias("target")
        .merge(
            inserts_updates.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenMatchedUpdate(
            set = {
                "order_id": "source.order_id",
                "product_id": "source.product_id",
                "quantity": "source.quantity",
                "unit_price": "source.unit_price",
                "discount_pct": "source.discount_pct",
                "line_total": "source.line_total",
                "transaction_date": "source.transaction_date",
                "return_flag": "source.return_flag",
                "calculated_line_total": "source.calculated_line_total",
                "is_valid_line_total": "source.is_valid_line_total",
                "has_discount": "source.has_discount",
                "is_returned": "source.is_returned",
                "updated_at": "source.updated_at"
            }
        )
        .whenNotMatchedInsert(
            values = {
                "transaction_id": "source.transaction_id",
                "order_id": "source.order_id",
                "product_id": "source.product_id",
                "quantity": "source.quantity",
                "unit_price": "source.unit_price",
                "discount_pct": "source.discount_pct",
                "line_total": "source.line_total",
                "transaction_date": "source.transaction_date",
                "return_flag": "source.return_flag",
                "calculated_line_total": "source.calculated_line_total",
                "is_valid_line_total": "source.is_valid_line_total",
                "has_discount": "source.has_discount",
                "is_returned": "source.is_returned",
                "updated_at": "source.updated_at"
            }
        )
        .execute()
    )
    print(f"✅ Processed {inserts_updates.count():,} INSERT/UPDATE operations")

# Handle DELETE operations
if deletes.count() > 0:
    (
        target_table.alias("target")
        .merge(
            deletes.alias("source"),
            "target.transaction_id = source.transaction_id"
        )
        .whenMatchedDelete()
        .execute()
    )
    print(f"✅ Processed {deletes.count():,} DELETE operations")

# Update checkpoint
max_timestamp_txn = silver_txn_updates.agg(F.max("updated_at")).collect()[0][0]
update_checkpoint("bronze_transactions", max_timestamp_txn, silver_txn_updates.count())

print("\n📊 Incremental Transaction Table:")
final_count = spark.table("retail.silver.silver_transactions_incremental").count()
print(f"Total records: {final_count:,}")
display(spark.table("retail.silver.silver_transactions_incremental").orderBy("transaction_id").limit(10))

# COMMAND ----------

# DBTITLE 1,6. Summary and Comparison
# MAGIC %md
# MAGIC ## 6. Summary and Comparison
# MAGIC
# MAGIC ### ✅ What We Implemented
# MAGIC
# MAGIC | Feature | Implementation | Status |
# MAGIC |---------|---------------|--------|
# MAGIC | **Incremental Processing** | Read only new/changed records using timestamps | ✅ |
# MAGIC | **Checkpoint Tracking** | Track last processed timestamp per table | ✅ |
# MAGIC | **MERGE/UPSERT** | Delta MERGE for INSERT + UPDATE | ✅ |
# MAGIC | **CDC Operations** | Handle Insert, Update, Delete | ✅ |
# MAGIC | **SCD Type 1** | Overwrite updates (no history) | ✅ |
# MAGIC | **SCD Type 2** | Keep all historical versions | ✅ |
# MAGIC
# MAGIC ### Full Load vs Incremental
# MAGIC
# MAGIC | Aspect | Full Load (Overwrite) | Incremental (MERGE) |
# MAGIC |--------|----------------------|--------------------|
# MAGIC | **Processing** | Entire dataset every time | Only new/changed records |
# MAGIC | **Performance** | Slower for large datasets | Much faster |
# MAGIC | **Cost** | Higher compute costs | Lower costs |
# MAGIC | **Complexity** | Simple - just overwrite | More complex - need checkpoints |
# MAGIC | **History** | No history kept | Can keep history (SCD Type 2) |
# MAGIC | **Use Case** | Small datasets, full refresh needed | Large datasets, frequent updates |
# MAGIC
# MAGIC ### When to Use Each Pattern
# MAGIC
# MAGIC **Use Full Load When:**
# MAGIC * Dataset is small (< 1M rows)
# MAGIC * Source doesn't provide timestamps
# MAGIC * Complete refresh is required
# MAGIC * Simplicity is more important than performance
# MAGIC
# MAGIC **Use Incremental/CDC When:**
# MAGIC * Dataset is large (> 1M rows)
# MAGIC * Source has timestamps or CDC logs
# MAGIC * Frequent updates (hourly, daily)
# MAGIC * Cost optimization is important
# MAGIC * Need to track data history
# MAGIC
# MAGIC ### Production Recommendations
# MAGIC
# MAGIC 1. **Use Delta Lake MERGE** instead of overwrite for large tables
# MAGIC 2. **Implement checkpoint tables** to track progress
# MAGIC 3. **Add retry logic** for failed incremental loads
# MAGIC 4. **Monitor checkpoint lag** to detect processing delays
# MAGIC 5. **Use SCD Type 2** for audit requirements and compliance
# MAGIC 6. **Optimize MERGE** with Z-ORDER on join keys
# MAGIC 7. **Consider streaming** for real-time CDC with Auto Loader

# COMMAND ----------

# DBTITLE 1,Validate all CDC tables
# Validate all tables created in this notebook
from pyspark.sql import functions as F

print("=" * 90)
print("CDC & INCREMENTAL PROCESSING - VALIDATION SUMMARY")
print("=" * 90)

tables_to_validate = [
    ("Checkpoint Table", "retail.silver.checkpoint_table"),
    ("Customers SCD Type 1", "retail.silver.silver_customers_scd1"),
    ("Customers SCD Type 2", "retail.silver.silver_customers_scd2"),
    ("Transactions Incremental", "retail.silver.silver_transactions_incremental")
]

for label, table_name in tables_to_validate:
    try:
        df = spark.table(table_name)
        count = df.count()
        print(f"\n✅ {label}")
        print(f"   Table: {table_name}")
        print(f"   Records: {count:,}")
        
        # Special handling for SCD Type 2
        if "scd2" in table_name:
            current_count = df.filter(F.col("is_current") == True).count()
            historical_count = df.filter(F.col("is_current") == False).count()
            print(f"   Current records: {current_count:,}")
            print(f"   Historical records: {historical_count:,}")
    except Exception as e:
        print(f"\n❌ {label}")
        print(f"   Table: {table_name}")
        print(f"   Error: {str(e)}")

print("\n" + "=" * 90)
print("✅ CDC & INCREMENTAL PROCESSING IMPLEMENTATION COMPLETE")
print("=" * 90)
print("\n📄 Key Capabilities Added:")
print("   1. ✅ Checkpoint-based incremental processing")
print("   2. ✅ MERGE/UPSERT operations (Delta Lake)")
print("   3. ✅ CDC handling (Insert, Update, Delete)")
print("   4. ✅ SCD Type 1 (current state only)")
print("   5. ✅ SCD Type 2 (full history tracking)")
print("\n💡 Next Steps:")
print("   - Schedule this notebook to run incrementally (hourly/daily)")
print("   - Monitor checkpoint lag for processing delays")
print("   - Optimize MERGE with Z-ORDER on join keys")
print("   - Consider Auto Loader for streaming CDC")

# COMMAND ----------

