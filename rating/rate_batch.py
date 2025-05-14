import psycopg2
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# --- Fonctions d’aide pour le rating ---
def is_peak_hour(ts):
    return 8 <= ts.hour < 20

def is_weekend(ts):
    return ts.weekday() >= 5

def is_international(peer_id):
    return peer_id and not peer_id.startswith("212")

def apply_loyalty_discount(base_cost, activation_date):
    if activation_date <= datetime.now().date() - timedelta(days=365):
        return base_cost * 0.95, "loyalty_5%"
    return base_cost, None

def apply_student_discount(base_cost, is_student):
    if is_student:
        return base_cost * 0.90, "student_10%"
    return base_cost, None

# --- Connexion à PostgreSQL ---
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# --- Lire les CDRs normalisés non encore traités ---
cursor.execute("""
    SELECT * FROM normalized_cdr
    WHERE record_hash NOT IN (SELECT record_hash FROM rated_cdr)
    AND status = 'ok'
""")
records = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]

for row in records:
    record = dict(zip(columns, row))
    msisdn = record['msisdn']
    service_type = record['record_type']
    record_hash = record['record_hash']

    # --- Chercher le client ---
    cursor.execute("""SELECT * FROM customers WHERE customer_id = %s AND status = 'active'""", (msisdn,))
    customer = cursor.fetchone()

    if not customer:
        rating_status = "unmatched"
        cursor.execute("""
            INSERT INTO rated_cdr (record_hash, msisdn, rating_status)
            VALUES (%s, %s, %s)
        """, (record_hash, msisdn, rating_status))
        continue

    customer_data = dict(zip([desc[0] for desc in cursor.description], customer))
    plan_id = customer_data['rate_plan_id']
    is_student = customer_data['is_student']
    activation_date = customer_data['activation_date']

    # --- Chercher le tarif ---
    cursor.execute("""
        SELECT unit_price, free_units FROM product_rates
        WHERE rate_plan_id = %s AND service_type = %s
    """, (plan_id, service_type))
    rate = cursor.fetchone()

    if not rate:
        cursor.execute("""
            INSERT INTO rated_cdr (record_hash, msisdn, rating_status)
            VALUES (%s, %s, %s)
        """, (record_hash, msisdn, 'unmatched'))
        continue

    unit_price, free_units = rate
    ts = record['timestamp']
    ts = ts if isinstance(ts, datetime) else datetime.fromisoformat(str(ts))

    base_cost = 0.0
    discount_applied = None

    if service_type == 'voice':
        duration = record.get('duration_sec', 0)
        base_cost = duration * unit_price
        if is_peak_hour(ts):
            base_cost *= 1.2
        if is_weekend(ts):
            base_cost *= 0.9
        if is_international(record.get('peer_id')):
            base_cost *= 2.0

    elif service_type == 'data':
        volume = record.get('data_volume_mb', 0.0)
        base_cost = volume * unit_price

    elif service_type == 'sms':
        base_cost = unit_price

    # --- Appliquer les réductions ---
    base_cost, disc1 = apply_loyalty_discount(base_cost, activation_date)
    base_cost, disc2 = apply_student_discount(base_cost, is_student)
    discounts = ','.join(filter(None, [disc1, disc2])) or None

    # --- Insertion dans rated_cdr ---
    cursor.execute("""
        INSERT INTO rated_cdr (
            record_hash, record_type, timestamp, msisdn, peer_id,
            duration_sec, data_volume_mb, session_duration_sec,
            cell_id, technology, status,
            customer_id, rate_plan_id, unit_price,
            base_cost, final_cost, discount_applied, rating_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s)
    """, (
        record_hash,
        service_type,
        ts,
        msisdn,
        record.get('peer_id'),
        record.get('duration_sec'),
        record.get('data_volume_mb'),
        record.get('session_duration_sec'),
        record.get('cell_id'),
        record.get('technology'),
        record.get('status'),
        msisdn,
        plan_id,
        unit_price,
        base_cost,
        base_cost,
        discounts,
        'rated'
    ))

conn.commit()
cursor.close()
conn.close()
