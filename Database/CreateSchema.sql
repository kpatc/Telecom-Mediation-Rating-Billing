---Creaate a database and a table to store normalized CDR records
CREATE TABLE normalized_cdr (
    record_type TEXT,
    timestamp TIMESTAMP,
    msisdn TEXT,
    peer_id TEXT,
    duration_sec INT,
    data_volume_mb FLOAT,
    session_duration_sec INT,
    cell_id TEXT,
    technology TEXT,
    status TEXT,
    record_hash TEXT PRIMARY KEY
);

-- rating_engine.sql
-- 1. Création des tables pour la facturation (PostgreSQL)

CREATE TABLE customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_name VARCHAR,
    subscription_type VARCHAR CHECK (subscription_type = 'postpaid'),
    rate_plan_id VARCHAR,
    activation_date DATE,
    status VARCHAR CHECK (status IN ('active', 'suspended', 'terminated')),
    region VARCHAR,
    is_student BOOLEAN DEFAULT FALSE
);

-- Plans tarifaires
CREATE TABLE rate_plans (
    rate_plan_id VARCHAR PRIMARY KEY,
    plan_name VARCHAR
);

-- Catalogue des produits
CREATE TABLE product_catalog (
    product_code VARCHAR PRIMARY KEY,
    service_type VARCHAR CHECK (service_type IN ('voice', 'sms', 'data')),
    unit VARCHAR,
    rate_type VARCHAR
);

-- Tarifs par service et plan
CREATE TABLE product_rates (
    rate_plan_id VARCHAR,
    service_type VARCHAR,
    unit_price FLOAT,
    free_units INT DEFAULT 0,
    PRIMARY KEY (rate_plan_id, service_type),
    FOREIGN KEY (rate_plan_id) REFERENCES rate_plans(rate_plan_id)
);

-- Enregistrements notés (rated usage)
CREATE TABLE rated_cdr (
    record_hash VARCHAR PRIMARY KEY,
    record_type VARCHAR,
    timestamp TIMESTAMP,
    msisdn VARCHAR,
    peer_id VARCHAR,
    duration_sec INT,
    data_volume_mb FLOAT,
    session_duration_sec INT,
    cell_id VARCHAR,
    technology VARCHAR,
    status VARCHAR,
    customer_id VARCHAR,
    rate_plan_id VARCHAR,
    unit_price FLOAT,
    base_cost FLOAT,
    final_cost FLOAT,
    discount_applied TEXT,
    rating_status VARCHAR
);

-- Factures générées
CREATE TABLE invoices (
    invoice_id SERIAL PRIMARY KEY,
    customer_id VARCHAR,
    billing_period DATE,
    total_base_cost FLOAT,
    total_discount FLOAT,
    tax_applied FLOAT,
    total_final_cost FLOAT,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
