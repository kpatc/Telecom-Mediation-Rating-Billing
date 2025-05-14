import psycopg2
from datetime import datetime
import json
import os
from dotenv import load_dotenv
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Paramètres
VAT_RATE = 0.20  # 20%
REGULATORY_FEE = 1.0  # MAD par client
DISCOUNT_STUDENT = 0.10
DISCOUNT_LOYALTY = 0.05
LOYALTY_MONTHS = 12

def get_billing_period():
    now = datetime.now()
    return datetime(now.year, now.month, now.day)

def run_billing():
    billing_period = get_billing_period()
    conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host="localhost",
    port="5432"
)
    cur = conn.cursor()

    # 1. Obtenir les clients actifs
    cur.execute("""
        SELECT customer_id, is_student, activation_date
        FROM customers
        WHERE status = 'active'
    """)
    customers = cur.fetchall()

    for customer_id, is_student, activation_date in customers:
        # Vérifier si la facture existe déjà pour ce client et cette période
        cur.execute("""
            SELECT COUNT(*) FROM invoices 
            WHERE customer_id = %s AND billing_period = %s
        """, (customer_id, billing_period))
        if cur.fetchone()[0] > 0:
            print(f"⚠️ Facture déjà générée pour {customer_id} sur {billing_period.strftime('%Y-%m')} — skipping.")
            continue  # Passer au client suivant

        # 2. Charger les usages facturables
        cur.execute("""
            SELECT final_cost FROM rated_cdr
            WHERE customer_id = %s AND rating_status = 'rated'
              AND date_trunc('month', timestamp) = %s
        """, (customer_id, billing_period))
        rows = cur.fetchall()

        if not rows:
            continue

        base_total = sum([r[0] for r in rows])
        discount = 0

        # 3. Fidélité ?
        active_months = (datetime.now().year - activation_date.year) * 12 + (datetime.now().month - activation_date.month)
        if active_months > LOYALTY_MONTHS:
            discount += base_total * DISCOUNT_LOYALTY

        # 4. Étudiant ?
        if is_student:
            discount += base_total * DISCOUNT_STUDENT

        # 5. Taxes
        taxed = (base_total - discount) * VAT_RATE
        total = base_total - discount + taxed + REGULATORY_FEE

        # 6. Enregistrement dans invoices
        cur.execute("""
            INSERT INTO invoices (
                customer_id, billing_period,
                total_base_cost, total_discount, tax_applied, total_final_cost
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            customer_id, billing_period,
            round(base_total, 2),
            round(discount, 2),
            round(taxed, 2),
            round(total, 2)
        ))

        # 7. Générer JSON
        facture = {
            "customer_id": customer_id,
            "billing_period": billing_period.strftime("%Y-%m"),
            "total_base_cost": round(base_total, 2),
            "discount": round(discount, 2),
            "taxes": round(taxed, 2),
            "regulatory_fee": REGULATORY_FEE,
            "total_due": round(total, 2)
        }

        os.makedirs("invoices", exist_ok=True)
        with open(f"invoices/invoice_{customer_id}_{billing_period.strftime('%Y_%m')}.json", "w") as f:
            json.dump(facture, f, indent=4)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Facturation terminée.")

if __name__ == "__main__":
    run_billing()
