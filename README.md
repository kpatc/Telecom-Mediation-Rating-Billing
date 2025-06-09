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

- 📈 **Dashboard & Reporting Module**  
  Comprehensive real-time business intelligence platform providing multi-dimensional insights into telecom operations. Features include:
  - Interactive revenue analysis and trend visualization
  - Advanced customer segmentation with RFM scoring
  - Regional revenue breakdown and geographical insights
  - Service-wise profitability and usage pattern analysis
  - Drill-down capabilities and exportable reports (PDF invoices, CSV datasets)
  - Responsive design for executive decision-making and operational oversight

## ⚙️ Tech Stack

- Apache Kafka 🟠 – real-time message broker  
- Apache Spark ⚡ – streaming data processing , and batch processing ,
- PostgreSQL 🐘 – persistent storage of customers, product catalog, normalized, rated, and billed data  
- Python 🐍 – glue logic, and data generation  
- Faker – for generating synthetic telecom records  
- Streamlit 🎯 – interactive web dashboard and business intelligence frontend
- Plotly 📈 – advanced data visualization and interactive charts  
- ReportLab 📄 – automated PDF invoice generation

## 🚧 Upcoming



---

> This project embraces modularity, distributed processing, and Big Data principles to simulate how real telecom systems handle large-scale data pipelines.
