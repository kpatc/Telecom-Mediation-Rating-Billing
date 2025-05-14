# 📡 Telecom Mediation, Rating & Billing System

This academic project simulates a complete telecom data pipeline using **Kafka**, **Apache Spark**, and **PostgreSQL**, covering the full lifecycle of telecom usage data — from raw event generation to monthly billing.

## ✅ Features Implemented

- 🧪 **Synthetic Data Generation**  
  Simulates realistic usage records (voice, SMS, data) across various mobile technologies (2G–5G) and customers.  
  Includes anomalies like missing fields, negative durations, out-of-order timestamps, and malformed numbers.  
  Records are pushed in real-time to Kafka for ingestion and testing.

- 🔄 **Mediation Engine (Streaming with Spark)**  
  Consumes CDRs from Kafka, cleanses and validates them, normalizes key fields (e.g., MSISDN), and writes valid records to both JSON and PostgreSQL.

- 💰 **Rating Engine (Batch)**  
  Applies detailed pricing logic to each usage event. Takes into account:
  - Service type & rate plans
  - Time-based modifiers (peak/off-peak)
  - Location (domestic/international)
  - Discounts (students, loyalty, first X SMS free)
  - Final cost computation and status tracking

- 🧾 **Billing Engine (Monthly Batch)**  
  Aggregates all rated events by customer and month, applies taxes (VAT), discounts, and fees to generate final invoice data (JSON output).

## ⚙️ Tech Stack

- Apache Kafka 🟠 – real-time message broker  
- Apache Spark ⚡ – streaming data processing  
- PostgreSQL 🐘 – persistent storage of normalized, rated, and billed data  
- Python 🐍 – glue logic, batch processing, and data generation  
- Faker – for generating synthetic telecom records  

## 🚧 Upcoming

- 📈 **Dashboard & Reporting Module**  
  Will provide insights into usage patterns, revenue, and system performance using modern data visualization tools (e.g., Superset, Metabase, or Dash).

---

> This project embraces modularity, distributed processing, and Big Data principles to simulate how real telecom systems handle large-scale data pipelines.
