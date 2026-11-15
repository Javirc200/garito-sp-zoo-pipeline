# garito-sp-zoo-pipeline

Welcome to this repository. This project was engineered and built by **Javier Rodriguez** — Economist and Data Engineer, and Founder of Garito.

### About the Project
Garito is an event management startup founded in 2026 that hosts parties and nightlife events at Warsaw's premier clubs, such as Blu Club, Room 13, Schodki, and Barka. 

This data pipeline was engineered to solve real world operational challenges for an active business. By integrating directly with **Fourvenues** (the ticketing and management software used by Garito), the system automates data centralization into **Databricks**. Some sensitive operational data has been anonymized to ensure complete legal and regulatory compliance.

The resulting analytical models directly power critical business operations, including **billing automation, accounting reconciliations, and executive BI dashboards**.

---

## 🏢 Business Context & Problem Statement

Running a high volume entertainment startup dealing with ticket sales, multi event passes, and venue wallet transactions requires reliable, automated financial tracking. This architecture was designed to solve key operational hurdles:
* **Billing & Accounting:** Centralizing clean financial transaction logs for automated reporting and daily financial closing.
* **Operational Analytics:** Tracking performance and attendance across venues and events (e.g., distinguishing standard tickets from multi-event passes like *Barka & Room 13*).
* **BI & Decision Making:** Feeding structured Star Schema models into downstream business intelligence tools for real-time revenue tracking and customer insights.

---

## 🏗️ Architecture Overview

The data flows through three strictly segregated analytical layers hosted within the **Unity Catalog**:

1. **Bronze Layer (`garito_bronze`):** Raw, immutable data ingestion from Fourvenues endpoints (`/payments/`, `/tickets/`, `/wallet-movements/`) utilizing automated pagination and serverless-safe Delta tables.
2. **Silver Layer (`garito_silver`):** Structural parsing, business logic enforcement, and strict **GDPR compliance** via cryptographic PII hashing (SHA-256 for customer emails and phone numbers).
3. **Gold Layer (`garito_gold`):** Dimensional modeling structured into an analytical **Star Schema** (`dim_events`, `fact_sales`, `fact_wallet_movements`) optimized for accounting and BI reporting.

---

## 🛠️ Tech Stack & Components

* **Orchestration & Compute:** Databricks Serverless Workflows (DAG-based scheduled jobs with task dependencies).
* **Storage & Format:** Delta Lake (Unity Catalog).
* **Language & Libraries:** Python, PySpark, Requests.
* **Security & Governance:** Databricks Widgets / Secrets Management and cryptographic anonymization (SHA-256).

---

## 📂 Project Structure

```text
├── 01_bronze_ingestion.py         # Automated API pagination & raw storage
├── 02_silver_cleaning_and_pii.py  # Data cleaning, business rules & GDPR masking
├── 03_gold_star_schema.py         # Dimensional modeling (Star Schema for BI/Billing)
└── README.md                      # Project documentation
