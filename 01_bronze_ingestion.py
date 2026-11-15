# Databricks notebook source
import json
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# ==========================================
# 1. SECURE CONFIGURATION VIA WIDGETS (secrets would be a better option but they are not available in free edition)
# ==========================================
dbutils.widgets.text("fourvenues_api_key_input", "", "Fourvenues API Key")
API_KEY = dbutils.widgets.get("fourvenues_api_key_input")

if not API_KEY:
    raise ValueError("Please enter your Fourvenues API Key in the widget box at the top of the notebook.")

BASE_URL = "https://api.fourvenues.com/integrations"

HEADERS = {
    "x-api-key": API_KEY.strip(),
    "Accept": "application/json"
}

def fetch_paginated_endpoint(endpoint: str, extra_params: dict = None, limit: int = 100) -> list:
    """
    Extracts all paginated records from a Fourvenues Integrations API endpoint.
    """
    all_records = []
    offset = 0
    params = dict(extra_params or {})
    params["limit"] = limit

    while True:
        params["offset"] = offset
        url = f"{BASE_URL}/{endpoint.strip('/')}/"
        response = requests.get(url, headers=HEADERS, params=params, timeout=30)

        if response.status_code != 200:
            raise Exception(f"Fourvenues API Error {response.status_code} on {endpoint}: {response.text[:300]}")

        payload = response.json()
        batch = payload.get("data", payload) if isinstance(payload, dict) else payload

        if not batch or not isinstance(batch, list):
            break

        all_records.extend(batch)

        if len(batch) < limit:
            break
        offset += limit

    return all_records

def ingest_to_bronze(records: list, table_name: str, endpoint_source: str):
    """
    Stores raw JSON payloads in a Bronze Delta table (Serverless-safe).
    """
    if not records:
        print(f"No records returned for {table_name}. Skipping write.")
        return

    schema = StructType([StructField("raw_payload", StringType(), True)])
    rows = [(json.dumps(r),) for r in records]
    df_raw = spark.createDataFrame(rows, schema=schema)

    df_bronze = (
        df_raw
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_system", F.lit("fourvenues_integrations_api"))
        .withColumn("_source_endpoint", F.lit(endpoint_source))
    )

    target_table = f"bronze_{table_name}"
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)

    print(f"Success: Ingested {df_bronze.count()} raw records into table '{target_table}'.")

# --- Execution ---
spark.sql("CREATE SCHEMA IF NOT EXISTS garito_bronze")
spark.sql("USE garito_bronze")

# 1. Payments
payments_data = fetch_paginated_endpoint("payments", extra_params={"start_date": "2025-01-01", "end_date": "2026-12-31"})
ingest_to_bronze(payments_data, "fourvenues_payments", "/payments/")

# 2. Tickets
unique_event_ids = list({p["event_id"] for p in payments_data if isinstance(p, dict) and p.get("event_id")})
all_tickets = []
for ev_id in unique_event_ids:
    ev_tickets = fetch_paginated_endpoint("tickets", extra_params={"event_id": ev_id})
    all_tickets.extend(ev_tickets)
ingest_to_bronze(all_tickets, "fourvenues_tickets", "/tickets/")

# 3. Wallet Movements
wallet_data = fetch_paginated_endpoint("wallet-movements", extra_params={"start_date": "2025-01-01", "end_date": "2026-12-31"})
ingest_to_bronze(wallet_data, "fourvenues_wallet_movements", "/wallet-movements/")