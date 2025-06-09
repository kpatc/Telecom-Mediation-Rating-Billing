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

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- PostgreSQL 12+
- Apache Kafka 2.8+
- Apache Spark 3.2+
- Git

### Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/kpatc/Telecom-Mediation-Rating-Billing.git
   cd Telecom-Mediation-Rating-Billing
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv telecom_env
   source telecom_env/bin/activate  # On Windows: telecom_env\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Database Setup**
   ```bash
   # Create PostgreSQL database and run schema
   psql -U postgres -c "CREATE DATABASE telecom_db;"
   psql -U postgres -d telecom_db -f Database/CreateSchema.sql
   ```

5. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials and Kafka settings
   ```

### Running the System

1. **Start Kafka & Zookeeper**
   ```bash
   # Start Zookeeper
   bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # Start Kafka
   bin/kafka-server-start.sh config/server.properties
   ```

2. **Generate synthetic data**
   ```bash
   cd generator
   python generator.py
   ```

3. **Run Mediation Engine**
   ```bash
   cd stream_mediation
   jupyter notebook streaming_mediation_notebook.ipynb
   # Or run the Spark streaming job directly
   ```

4. **Execute Rating Engine**
   ```bash
   cd rating
   python rating_engine.py
   ```

5. **Run Billing Engine**
   ```bash
   cd billing
   python billing_engine.py
   ```

6. **Launch Analytics Dashboard**
   ```bash
   cd dashboard
   streamlit run app.py
   ```
   Access the dashboard at `http://localhost:8501`

### Dashboard Features
The Streamlit dashboard provides:
- **Overview**: Real-time KPIs and system health metrics
- **Revenue Analytics**: Interactive charts and trend analysis
- **Customer Insights**: RFM segmentation and behavioral analytics
- **Billing Management**: Invoice generation and PDF export capabilities

Navigate between pages using the sidebar menu for comprehensive business intelligence insights.

---

> This project embraces modularity, distributed processing, and Big Data principles to simulate how real telecom systems handle large-scale data pipelines.
