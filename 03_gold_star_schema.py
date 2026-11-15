# Databricks notebook source
from pyspark.sql import functions as F

# Set working schema to Gold layer
spark.sql("CREATE SCHEMA IF NOT EXISTS garito_gold")
spark.sql("USE garito_gold")

# ==========================================
# 1. DIMENSION: EVENTS & PASSES (dim_events)
# ==========================================
df_payments = spark.read.table("garito_silver.silver_fourvenues_payments")

# Build event dimension combining regular events and multi-event passes
df_dim_events = df_payments.select(
    F.coalesce(F.col("event_id"), F.lit("multievent_pass")).alias("event_key"),
    F.col("event_slug"),
    F.col("event_date"),
    F.col("is_pass")
).distinct()

df_dim_events.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_events")

print(f"Success: Created 'dim_events' with {df_dim_events.count()} records.")


# ==========================================
# 2. FACT TABLE: SALES & REVENUE (fact_sales)
# ==========================================
df_fact_sales = df_payments.select(
    F.col("payment_id"),
    F.coalesce(F.col("event_id"), F.lit("multievent_pass")).alias("event_key"),
    F.col("sale_date"),
    F.col("total_amount"),
    F.col("total_fees"),
    F.col("n_resources"),
    F.col("is_pass"),
    F.col("customer_email_hashed")
)

df_fact_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_sales")

print(f"Success: Created 'fact_sales' with {df_fact_sales.count()} records.")


# ==========================================
# 3. FACT TABLE: WALLET & TREASURY (fact_wallet_movements)
# ==========================================
df_wallet = spark.read.table("garito_silver.silver_fourvenues_wallet_movements")

df_fact_wallet = df_wallet.select(
    F.col("movement_id"),
    F.col("movement_date"),
    F.col("concept"),
    F.col("category"),
    F.col("net_amount")
)

df_fact_wallet.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_wallet_movements")

print(f"Success: Created 'fact_wallet_movements' with {df_fact_wallet.count()} records.")