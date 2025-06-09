from pyspark.sql import SparkSession
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
    return datetime(now.year, now.month, 1)  # Premier jour du mois

def create_spark_session():
    return SparkSession.builder \
        .appName("TelecomBilling") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def read_postgres_table(spark, table_name, where_clause=""):
    jdbc_url = f"jdbc:postgresql://localhost:5432/{DB_NAME}"
    properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    
    if where_clause:
        query = f"(SELECT * FROM {table_name} WHERE {where_clause}) as t"
        return spark.read.jdbc(jdbc_url, query, properties=properties)
    else:
        return spark.read.jdbc(jdbc_url, table_name, properties=properties)

def write_to_postgres(df, table_name):
    jdbc_url = f"jdbc:postgresql://localhost:5432/{DB_NAME}"
    properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    df.write.mode("append").jdbc(jdbc_url, table_name, properties=properties)

def calculate_free_units_adjustment(spark, customer_id, rate_plan_id, billing_period_str):
    """Calculer l'ajustement pour les unités gratuites avec Spark"""
    adjustments = {}
    
    # Récupérer les free_units pour ce plan
    product_rates_df = read_postgres_table(spark, "product_rates", 
                                         f"rate_plan_id = '{rate_plan_id}' AND free_units > 0")
    
    if product_rates_df.count() == 0:
        return adjustments
    
    free_units_plans = product_rates_df.collect()
    
    for row in free_units_plans:
        service_type = row.service_type
        unit_price = row.unit_price
        free_units = row.free_units
        
        # Calculer l'usage total pour ce service ce mois
        if service_type == 'sms':
            usage_df = read_postgres_table(spark, "rated_cdr", 
                f"customer_id = '{customer_id}' AND record_type = '{service_type}' AND date_trunc('month', timestamp) = '{billing_period_str}' AND rating_status = 'rated'")
            
            total_usage = usage_df.count()
            
            # Calculer le crédit pour les unités gratuites
            free_units_used = min(total_usage, free_units)
            credit_amount = free_units_used * unit_price
            
            adjustments[service_type] = {
                'total_usage': total_usage,
                'free_units': free_units,
                'free_units_used': free_units_used,
                'credit_amount': credit_amount
            }
        
        elif service_type == 'voice':
            usage_df = read_postgres_table(spark, "rated_cdr", 
                f"customer_id = '{customer_id}' AND record_type = '{service_type}' AND date_trunc('month', timestamp) = '{billing_period_str}' AND rating_status = 'rated'")
            
            total_seconds = usage_df.agg({"duration_sec": "sum"}).collect()[0][0] or 0
            
            # Convertir free_units en secondes
            free_seconds = free_units * 60
            free_seconds_used = min(total_seconds, free_seconds)
            credit_amount = free_seconds_used * unit_price
            
            adjustments[service_type] = {
                'total_usage': total_seconds,
                'free_units': free_seconds,
                'free_units_used': free_seconds_used,
                'credit_amount': credit_amount
            }
            
        elif service_type == 'data':
            usage_df = read_postgres_table(spark, "rated_cdr", 
                f"customer_id = '{customer_id}' AND record_type = '{service_type}' AND date_trunc('month', timestamp) = '{billing_period_str}' AND rating_status = 'rated'")
            
            total_mb = usage_df.agg({"data_volume_mb": "sum"}).collect()[0][0] or 0
            
            free_mb_used = min(total_mb, free_units)
            credit_amount = free_mb_used * unit_price
            
            adjustments[service_type] = {
                'total_usage': total_mb,
                'free_units': free_units,
                'free_units_used': free_mb_used,
                'credit_amount': credit_amount
            }
    
    return adjustments

def run_spark_billing():
    billing_period = get_billing_period()
    billing_period_str = billing_period.strftime('%Y-%m-%d')
    spark = create_spark_session()
    
    print(f"Starting billing for period: {billing_period.strftime('%Y-%m')}")
    
    try:
        # 1. Obtenir les clients actifs avec leur plan tarifaire
        customers_df = read_postgres_table(spark, "customers", "status = 'active'")
        customers = customers_df.collect()

        total_invoices = 0
        
        for customer_row in customers:
            customer_id = customer_row.customer_id
            is_student = customer_row.is_student
            activation_date = customer_row.activation_date
            rate_plan_id = customer_row.rate_plan_id
            
            # Vérifier si la facture existe déjà
            existing_df = read_postgres_table(spark, "invoices", 
                f"customer_id = '{customer_id}' AND billing_period = '{billing_period_str}'")
            
            if existing_df.count() > 0:
                print(f"⚠️ Facture déjà générée pour {customer_id} sur {billing_period.strftime('%Y-%m')} — skipping.")
                continue

            # 2. Charger les usages facturables
            usage_df = read_postgres_table(spark, "rated_cdr", 
                f"customer_id = '{customer_id}' AND rating_status = 'rated' AND date_trunc('month', timestamp) = '{billing_period_str}'")

            if usage_df.count() == 0:
                print(f"ℹ️ Aucun usage pour {customer_id} ce mois")
                continue

            base_total = usage_df.agg({"final_cost": "sum"}).collect()[0][0] or 0
            
            # 3. Calculer les ajustements pour les unités gratuites
            free_units_adjustments = calculate_free_units_adjustment(
                spark, customer_id, rate_plan_id, billing_period_str
            )
            
            # Calculer le crédit total des unités gratuites
            free_units_credit = sum([adj['credit_amount'] for adj in free_units_adjustments.values()])
            
            # Appliquer le crédit des unités gratuites
            adjusted_total = max(0, base_total - free_units_credit)
            
            discount = 0

            # 4. Fidélité
            active_months = (datetime.now().year - activation_date.year) * 12 + (datetime.now().month - activation_date.month)
            if active_months > LOYALTY_MONTHS:
                discount += adjusted_total * DISCOUNT_LOYALTY

            # 5. Étudiant
            if is_student:
                discount += adjusted_total * DISCOUNT_STUDENT

            # 6. Taxes
            taxable_amount = max(0, adjusted_total - discount)
            tax_applied = taxable_amount * VAT_RATE
            total_final = taxable_amount + tax_applied + REGULATORY_FEE

            # 7. Enregistrement dans invoices
            invoice_df = spark.createDataFrame([
                (customer_id, billing_period, round(base_total, 2), 
                 round(discount + free_units_credit, 2), round(tax_applied, 2), round(total_final, 2))
            ], ["customer_id", "billing_period", "total_base_cost", "total_discount", "tax_applied", "total_final_cost"])
            
            write_to_postgres(invoice_df, "invoices")

            # 8. Générer JSON détaillé
            facture = {
                "customer_id": customer_id,
                "rate_plan_id": rate_plan_id,
                "billing_period": billing_period.strftime("%Y-%m"),
                "usage_summary": {
                    "total_base_cost": round(base_total, 2),
                    "free_units_credit": round(free_units_credit, 2),
                    "adjusted_usage_cost": round(adjusted_total, 2)
                },
                "free_units_details": {
                    service: {
                        "total_usage": details['total_usage'],
                        "free_allowance": details['free_units'],
                        "free_used": details['free_units_used'],
                        "credit_applied": round(details['credit_amount'], 2)
                    }
                    for service, details in free_units_adjustments.items()
                },
                "discounts": {
                    "loyalty_discount": round(adjusted_total * DISCOUNT_LOYALTY, 2) if active_months > LOYALTY_MONTHS else 0,
                    "student_discount": round(adjusted_total * DISCOUNT_STUDENT, 2) if is_student else 0,
                    "total_discounts": round(discount, 2)
                },
                "charges": {
                    "subtotal_after_discounts": round(taxable_amount, 2),
                    "vat_20%": round(tax_applied, 2),
                    "regulatory_fee": REGULATORY_FEE,
                    "total_due": round(total_final, 2)
                }
            }

            os.makedirs("invoices", exist_ok=True)
            filename = f"invoices/invoice_{customer_id}_{billing_period.strftime('%Y_%m')}.json"
            with open(filename, "w") as f:
                json.dump(facture, f, indent=4)

            total_invoices += 1
            print(f"✅ Facture générée pour {customer_id}: {round(total_final, 2)} MAD")

        print(f"✅ Facturation terminée. {total_invoices} factures générées.")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_spark_billing()