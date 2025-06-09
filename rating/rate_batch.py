from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER") 
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Configuration de connexion PostgreSQL
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

postgres_url = f"jdbc:postgresql://localhost:5432/{DB_NAME}"

# Initialiser Spark
spark = SparkSession.builder \
    .appName("TelecomRatingEngine") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# --- Fonctions d'aide pour le rating avec gestion des None ---
def is_peak_hour(hour):
    if hour is None:
        return False
    return 8 <= hour < 20

def is_weekend(weekday):
    if weekday is None:
        return False
    return weekday >= 5

def is_international(peer_id):
    if peer_id is None:
        return False
    return not peer_id.startswith("212")

def calculate_voice_cost(duration_sec, unit_price, hour, weekday, peer_id):
    """Calculer le coût des appels vocaux avec gestion des valeurs None"""
    if duration_sec is None or unit_price is None:
        return 0.0
    
    base_cost = float(duration_sec) * float(unit_price)
    
    # Peak hour multiplier
    if hour is not None and 8 <= hour < 20:
        base_cost *= 1.2
    
    # Weekend discount
    if weekday is not None and weekday >= 5:
        base_cost *= 0.9
    
    # International multiplier
    if peer_id is not None and not peer_id.startswith("212"):
        base_cost *= 2.0
    
    return base_cost

def calculate_data_cost(data_volume_mb, unit_price):
    """Calculer le coût des données"""
    if data_volume_mb is None or unit_price is None:
        return 0.0
    return float(data_volume_mb) * float(unit_price)

def calculate_sms_cost(unit_price):
    """Calculer le coût des SMS"""
    if unit_price is None:
        return 0.0
    return float(unit_price)

def apply_discounts(base_cost, activation_date, is_student):
    """Appliquer les remises avec gestion des None"""
    if base_cost is None:
        return 0.0, None
    
    final_cost = float(base_cost)
    discounts = []
    
    # Loyalty discount
    if activation_date is not None:
        cutoff_date = datetime.now().date() - timedelta(days=365)
        if activation_date <= cutoff_date:
            final_cost *= 0.95
            discounts.append("loyalty_5%")
    
    # Student discount
    if is_student:
        final_cost *= 0.90
        discounts.append("student_10%")
    
    discount_text = ",".join(discounts) if discounts else None
    return final_cost, discount_text

# Enregistrer les UDFs avec gestion des types
calculate_voice_cost_udf = udf(calculate_voice_cost, FloatType())
calculate_data_cost_udf = udf(calculate_data_cost, FloatType())
calculate_sms_cost_udf = udf(calculate_sms_cost, FloatType())

apply_discounts_udf = udf(apply_discounts, StructType([
    StructField("final_cost", FloatType(), True),
    StructField("discounts", StringType(), True)
]))

def main():
    print("Starting Spark Rating Engine...")
    
    # 1. Charger les données de référence
    print("Loading reference data...")
    
    customers_df = spark.read.jdbc(
        url=postgres_url,
        table="customers",
        properties=postgres_properties
    ).filter(col("status") == "active").cache()
    
    product_rates_df = spark.read.jdbc(
        url=postgres_url,
        table="product_rates", 
        properties=postgres_properties
    ).cache()
    
    # 2. Charger les CDR non traités
    print("Loading unprocessed CDR records...")
    
    # Récupérer les record_hash déjà traités pour éviter les doublons
    try:
        rated_hashes_df = spark.read.jdbc(
            url=postgres_url,
            table="rated_cdr",
            properties=postgres_properties
        ).select("record_hash")
    except:
        # Si la table est vide, créer un DataFrame vide
        rated_hashes_df = spark.createDataFrame([], StructType([StructField("record_hash", StringType(), True)]))
    
    # Charger les CDR normalisés
    normalized_cdr_df = spark.read.jdbc(
        url=postgres_url,
        table="normalized_cdr",
        properties=postgres_properties
    ).filter(col("status") == "ok")
    
    # Exclure les enregistrements déjà traités
    unprocessed_cdr_df = normalized_cdr_df.join(
        rated_hashes_df, 
        normalized_cdr_df.record_hash == rated_hashes_df.record_hash,
        "left_anti"
    )
    
    record_count = unprocessed_cdr_df.count()
    print(f"Found {record_count} unprocessed records")
    
    if record_count == 0:
        print("No records to process. Exiting.")
        spark.stop()
        return
    
    # 3. Joindre avec les données client et tarifs
    print("Joining with customer and rate data...")
    
    # Créer des alias pour éviter les conflits
    cdr_alias = unprocessed_cdr_df.alias("cdr")
    customers_alias = customers_df.alias("customers")
    rates_alias = product_rates_df.alias("rates")
    
    # Jointure complète
    joined_df = cdr_alias \
        .join(customers_alias, 
              col("cdr.msisdn") == col("customers.customer_id"), "left") \
        .join(rates_alias, 
              (col("customers.rate_plan_id") == col("rates.rate_plan_id")) &
              (col("cdr.record_type") == col("rates.service_type")), "left")
    
    # 4. Séparer les enregistrements matchés et non-matchés
    matched_df = joined_df.filter(
        col("customers.customer_id").isNotNull() & col("rates.unit_price").isNotNull()
    )
    
    unmatched_df = joined_df.filter(
        col("customers.customer_id").isNull() | col("rates.unit_price").isNull()
    )
    
    # 5. Traitement des enregistrements matchés
    print("Calculating costs...")
    
    # Ajouter les colonnes temporelles
    matched_df = matched_df \
        .withColumn("hour", hour(col("cdr.timestamp"))) \
        .withColumn("weekday", dayofweek(col("cdr.timestamp")) - 1)
    
    # Calcul des coûts selon le type de service
    rated_df = matched_df \
        .withColumn("base_cost",
                   when(col("cdr.record_type") == "voice",
                        calculate_voice_cost_udf(
                            col("cdr.duration_sec"), 
                            col("rates.unit_price"),
                            col("hour"),
                            col("weekday"), 
                            col("cdr.peer_id")
                        ))
                   .when(col("cdr.record_type") == "data",
                        calculate_data_cost_udf(
                            col("cdr.data_volume_mb"), 
                            col("rates.unit_price")
                        ))
                   .when(col("cdr.record_type") == "sms",
                        calculate_sms_cost_udf(col("rates.unit_price")))
                   .otherwise(0.0))
    
    # Appliquer les remises
    rated_df = rated_df \
        .withColumn("discount_result",
                   apply_discounts_udf(
                       col("base_cost"),
                       col("customers.activation_date"),
                       col("customers.is_student")
                   )) \
        .withColumn("final_cost", col("discount_result.final_cost")) \
        .withColumn("discount_applied", col("discount_result.discounts")) \
        .withColumn("rating_status", lit("rated"))
    
    # 6. Préparer les DataFrames pour l'insertion
    print("Preparing data for insertion...")
    
    # DataFrame des enregistrements traités avec succès
    final_rated_df = rated_df.select(
        col("cdr.record_hash"),
        col("cdr.record_type"), 
        col("cdr.timestamp"),
        col("cdr.msisdn"),
        col("cdr.peer_id"),
        col("cdr.duration_sec"),
        col("cdr.data_volume_mb"),
        col("cdr.session_duration_sec"),
        col("cdr.cell_id"),
        col("cdr.technology"),
        col("cdr.status"),
        col("customers.customer_id"),
        col("customers.rate_plan_id"),
        col("rates.unit_price"),
        col("base_cost"),
        col("final_cost"),
        col("discount_applied"),
        col("rating_status")
    )
    
    # DataFrame des enregistrements non-matchés
    unmatched_final_df = unmatched_df.select(
        col("cdr.record_hash"),
        lit(None).alias("record_type"),
        lit(None).cast(TimestampType()).alias("timestamp"),
        col("cdr.msisdn"),
        lit(None).alias("peer_id"),
        lit(None).cast(IntegerType()).alias("duration_sec"),
        lit(None).cast(FloatType()).alias("data_volume_mb"),
        lit(None).cast(IntegerType()).alias("session_duration_sec"),
        lit(None).alias("cell_id"),
        lit(None).alias("technology"),
        lit(None).alias("status"),
        lit(None).alias("customer_id"),
        lit(None).alias("rate_plan_id"),
        lit(None).cast(FloatType()).alias("unit_price"),
        lit(None).cast(FloatType()).alias("base_cost"),
        lit(None).cast(FloatType()).alias("final_cost"),
        lit(None).alias("discount_applied"),
        lit("unmatched").alias("rating_status")
    )
    
    # Combiner tous les résultats
    all_results_df = final_rated_df.union(unmatched_final_df)
    
    # 7. Sauvegarder dans la base de données
    print("Saving results to database...")
    
    all_results_df.write \
        .mode("append") \
        .jdbc(
            url=postgres_url,
            table="rated_cdr",
            properties=postgres_properties
        )
    
    # 8. Afficher les statistiques
    total_processed = all_results_df.count()
    rated_count = final_rated_df.count()
    unmatched_count = unmatched_final_df.count()
    
    print(f"Rating completed!")
    print(f"Total processed: {total_processed}")
    print(f"Successfully rated: {rated_count}")
    print(f"Unmatched: {unmatched_count}")
    
    spark.stop()

if __name__ == "__main__":
    main()