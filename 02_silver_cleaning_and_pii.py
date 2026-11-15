# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# Set working schema to Silver layer
spark.sql("CREATE SCHEMA IF NOT EXISTS garito_silver")
spark.sql("USE garito_silver")

# ==========================================
# 1. PROCESS PAYMENTS & MULTI-EVENT PASSES
# ==========================================
df_bronze_payments = spark.read.table("garito_bronze.bronze_fourvenues_payments")

# Parse structural fields from the raw JSON payload
df_payments_parsed = df_bronze_payments.select(
    F.get_json_object("raw_payload", "$._id").alias("payment_id"),
    F.get_json_object("raw_payload", "$.event_id").alias("event_id"),
    F.get_json_object("raw_payload", "$.event_slug").alias("event_slug"),
    F.get_json_object("raw_payload", "$.event_date").alias("event_date"),
    F.get_json_object("raw_payload", "$.total_amount").cast(DoubleType()).alias("total_amount"),
    F.get_json_object("raw_payload", "$.total_fees").cast(DoubleType()).alias("total_fees"),
    F.get_json_object("raw_payload", "$.n_resources").cast(IntegerType()).alias("n_resources"),
    F.get_json_object("raw_payload", "$.resource_type").alias("resource_type"),
    F.get_json_object("raw_payload", "$.rate_name").alias("rate_name"),
    F.coalesce(
        F.get_json_object("raw_payload", "$.at"), 
        F.get_json_object("raw_payload", "$.created_at")
    ).alias("payment_timestamp"),
    # Raw PII fields for GDPR masking
    F.get_json_object("raw_payload", "$.customer_email").alias("customer_email"),
    F.get_json_object("raw_payload", "$.customer_phone").alias("customer_phone")
)

# Apply business logic rules and GDPR hashing (SHA-256)
df_silver_payments = df_payments_parsed \
    .withColumn("sale_date", F.to_date(F.col("payment_timestamp"))) \
    .withColumn(
        "is_pass",
        F.when(
            F.col("event_id").isNull() | F.lower(F.coalesce(F.col("resource_type"), F.lit(""))).contains("pass"),
            True
        ).otherwise(False)
    ) \
    .withColumn(
        "customer_email_hashed",
        F.when(F.col("customer_email").isNotNull(), F.sha2(F.lower(F.trim(F.col("customer_email"))), 256)).otherwise(None)
    ) \
    .withColumn(
        "customer_phone_hashed",
        F.when(F.col("customer_phone").isNotNull(), F.sha2(F.trim(F.col("customer_phone")), 256)).otherwise(None)
    ) \
    .drop("customer_email", "customer_phone") # Drop plain-text PII

df_silver_payments.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_fourvenues_payments")

print(f"Success: Processed {df_silver_payments.count()} records into 'silver_silver_payments'.")


# ==========================================
# 2. PROCESS TICKETS
# ==========================================
df_bronze_tickets = spark.read.table("garito_bronze.bronze_fourvenues_tickets")

df_silver_tickets = df_bronze_tickets.select(
    F.get_json_object("raw_payload", "$._id").alias("ticket_id"),
    F.get_json_object("raw_payload", "$.event_id").alias("event_id"),
    F.get_json_object("raw_payload", "$.rate_name").alias("rate_name"),
    F.get_json_object("raw_payload", "$.type").alias("ticket_type"),
    F.coalesce(
        F.get_json_object("raw_payload", "$.price").cast(DoubleType()),
        F.get_json_object("raw_payload", "$.amount").cast(DoubleType()),
        F.lit(0.0)
    ).alias("final_price")
)

df_silver_tickets.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_fourvenues_tickets")

print(f"Success: Processed {df_silver_tickets.count()} records into 'silver_fourvenues_tickets'.")


# ==========================================
# 3. PROCESS WALLET MOVEMENTS
# ==========================================
df_bronze_wallet = spark.read.table("garito_bronze.bronze_fourvenues_wallet_movements")

df_silver_wallet = df_bronze_wallet.select(
    F.get_json_object("raw_payload", "$._id").alias("movement_id"),
    F.get_json_object("raw_payload", "$.concept").alias("concept"),
    F.get_json_object("raw_payload", "$.category").alias("category"),
    F.to_date(
        F.coalesce(
            F.get_json_object("raw_payload", "$.operation_date"),
            F.get_json_object("raw_payload", "$.created_at")
        )
    ).alias("movement_date"),
    F.coalesce(
        F.get_json_object("raw_payload", "$.operation_amount").cast(DoubleType()),
        F.get_json_object("raw_payload", "$.amount").cast(DoubleType()),
        F.lit(0.0)
    ).alias("net_amount")
)

df_silver_wallet.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_fourvenues_wallet_movements")

print(f"Success: Processed {df_silver_wallet.count()} records into 'silver_fourvenues_wallet_movements'.")