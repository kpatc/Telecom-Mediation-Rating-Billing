# 📡 Telecom Mediation, Rating & Billing System

This academic project simulates a real-world telecom data pipeline using **Kafka**, **Apache Spark**, and **PostgreSQL**. It covers the full lifecycle from raw CDR (Call Detail Records) generation to cost computation (rating) and monthly billing.

## ✅ Features Implemented

- 🔄 **Mediation Engine (Streaming with Spark)**  
  Cleanses, validates, and normalizes incoming CDR events in real time from Kafka.

- 📊 **Rating Engine (Batch)**  
  Applies pricing logic to each usage event (voice, SMS, data) based on customer rate plans, discounts, and zones.

- 🧾 **Billing Engine (Batch)**  
  Aggregates rated records monthly to generate invoices (JSON) per customer with tax, discount, and fee calculations.

## ⚙️ Tech Stack

- Apache Kafka 🟠  
- Apache Spark (Structured Streaming) ⚡  
- PostgreSQL 🐘  
- Python + Faker + psycopg2 🐍  

## 🚧 Upcoming

- 📈 **Dashboard & Reporting Module**  
  A visual interface to monitor usage data, revenue trends, and system performance metrics.

---

> Built with modularity and big data principles in mind, this project simulates how real-world telecom systems handle massive amounts of event data.

