#!/usr/bin/env python3
"""
Script d'optimisation de la base de données
Ajoute des index pour améliorer les performances des requêtes
"""

import psycopg2
from config import DB_CONFIG
import streamlit as st

def create_performance_indexes():
    """Créer les index pour améliorer les performances"""
    
    indexes_to_create = [
        # Index pour la table customers
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_status ON customers(status);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_activation_date ON customers(activation_date);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_region ON customers(region);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_is_student ON customers(is_student);",
        
        # Index pour la table invoices
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoices_customer_id ON invoices(customer_id);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoices_billing_period ON invoices(billing_period);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoices_customer_billing ON invoices(customer_id, billing_period);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_invoices_total_final_cost ON invoices(total_final_cost);",
        
        # Index pour la table rated_cdr
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rated_cdr_customer_id ON rated_cdr(customer_id);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rated_cdr_timestamp ON rated_cdr(timestamp);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rated_cdr_rating_status ON rated_cdr(rating_status);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rated_cdr_record_type ON rated_cdr(record_type);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rated_cdr_customer_timestamp ON rated_cdr(customer_id, timestamp);",
        
        # Index pour la table normalized_cdr
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_normalized_cdr_created_at ON normalized_cdr(created_at);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_normalized_cdr_status ON normalized_cdr(status);",
    ]
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("🚀 Création des index de performance...")
        
        for i, index_sql in enumerate(indexes_to_create, 1):
            try:
                print(f"  [{i}/{len(indexes_to_create)}] Création de l'index...")
                cursor.execute(index_sql)
                conn.commit()
                print(f"  ✅ Index créé avec succès")
            except psycopg2.Error as e:
                print(f"  ⚠️  Erreur lors de la création de l'index: {e}")
                conn.rollback()
                continue
        
        # Analyser les tables pour mettre à jour les statistiques
        print("\n📊 Mise à jour des statistiques des tables...")
        tables_to_analyze = ['customers', 'invoices', 'rated_cdr', 'normalized_cdr']
        
        for table in tables_to_analyze:
            try:
                cursor.execute(f"ANALYZE {table};")
                conn.commit()
                print(f"  ✅ Statistiques mises à jour pour {table}")
            except psycopg2.Error as e:
                print(f"  ⚠️  Erreur lors de l'analyse de {table}: {e}")
                conn.rollback()
        
        cursor.close()
        conn.close()
        
        print("\n🎉 Optimisation de la base de données terminée!")
        print("💡 Les performances des requêtes devraient être améliorées.")
        
    except psycopg2.Error as e:
        print(f"❌ Erreur de connexion à la base de données: {e}")

def show_database_stats():
    """Afficher les statistiques de la base de données"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("\n📈 Statistiques de la base de données:")
        print("=" * 50)
        
        # Taille des tables
        cursor.execute("""
            SELECT 
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                pg_stat_get_tuples_returned(pg_class.oid) as tuples_read,
                pg_stat_get_tuples_fetched(pg_class.oid) as tuples_fetched
            FROM pg_tables 
            JOIN pg_class ON pg_class.relname = pg_tables.tablename
            WHERE schemaname = 'public'
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
        """)
        
        results = cursor.fetchall()
        print("\n🗂️  Taille des tables:")
        for row in results:
            print(f"  {row[1]}: {row[2]} ({row[3]} lectures, {row[4]} récupérations)")
        
        # Index existants
        cursor.execute("""
            SELECT 
                tablename,
                indexname,
                pg_size_pretty(pg_relation_size(indexname::regclass)) as index_size
            FROM pg_indexes 
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname;
        """)
        
        results = cursor.fetchall()
        print("\n📋 Index existants:")
        current_table = None
        for row in results:
            if row[0] != current_table:
                print(f"\n  📊 {row[0]}:")
                current_table = row[0]
            print(f"    - {row[1]} ({row[2]})")
        
        cursor.close()
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ Erreur lors de la récupération des statistiques: {e}")

if __name__ == "__main__":
    print("🔧 Optimisation de la base de données Telecom")
    print("=" * 50)
    
    # Afficher les statistiques actuelles
    show_database_stats()
    
    # Demander confirmation
    response = input("\n❓ Voulez-vous créer les index de performance? (y/N): ")
    
    if response.lower() in ['y', 'yes', 'oui']:
        create_performance_indexes()
        print("\n📊 Nouvelles statistiques:")
        show_database_stats()
    else:
        print("🚫 Optimisation annulée")
