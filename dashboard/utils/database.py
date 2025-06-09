import pandas as pd
import psycopg2
from config import DB_CONFIG
import streamlit as st

def get_connection():
    """Créer une connexion à la base de données"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données: {e}")
        return None

@st.cache_data(ttl=300)
def execute_query(query, params=None):
    """Exécuter une requête et retourner un DataFrame"""
    # Ne pas cacher la connexion, la créer à chaque fois
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erreur lors de l'exécution de la requête: {e}")
        return pd.DataFrame()

# Requêtes pré-définies
@st.cache_data(ttl=300)
def get_revenue_metrics():
    """Métriques de revenus"""
    query = """
    SELECT 
        DATE_TRUNC('month', billing_period) as month,
        SUM(total_final_cost) as total_revenue,
        COUNT(*) as total_invoices,
        AVG(total_final_cost) as avg_invoice
    FROM invoices 
    WHERE billing_period >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY DATE_TRUNC('month', billing_period)
    ORDER BY month
    """
    return execute_query(query)

@st.cache_data(ttl=600)  # Cache de 10 minutes pour les métriques clients
def get_customer_metrics():
    """Métriques clients optimisées"""
    query = """
    SELECT 
        COUNT(*) as total_customers,
        COUNT(CASE WHEN status = 'active' THEN 1 END) as active_customers,
        COUNT(CASE WHEN is_student = true THEN 1 END) as student_customers,
        AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, activation_date))) as avg_customer_age_years
    FROM customers
    """
    return execute_query(query)

@st.cache_data(ttl=300)
def get_usage_by_service():
    """Usage par service"""
    query = """
    SELECT 
        record_type,
        COUNT(*) as total_records,
        SUM(final_cost) as total_cost,
        AVG(final_cost) as avg_cost
    FROM rated_cdr 
    WHERE rating_status = 'rated'
        AND timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY record_type
    """
    return execute_query(query)

@st.cache_data(ttl=300)
def get_processing_metrics():
    """Métriques de traitement"""
    query = """
    SELECT 
        DATE(created_at) as date,
        status,
        COUNT(*) as count
    FROM normalized_cdr 
    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY DATE(created_at), status
    ORDER BY date
    """
    return execute_query(query)

@st.cache_data(ttl=600)  # Augmentation du cache pour les top customers
def get_top_customers(limit=10):
    """Top clients par revenus - Optimisé"""
    query = """
    SELECT 
        c.customer_id,
        SUM(i.total_final_cost) as total_spent,
        COUNT(i.invoice_id) as total_invoices,
        AVG(i.total_final_cost) as avg_invoice
    FROM customers c
    JOIN invoices i ON c.customer_id = i.customer_id
    WHERE i.billing_period >= CURRENT_DATE - INTERVAL '6 months'
      AND c.status = 'active'  -- Filtrer seulement les clients actifs
    GROUP BY c.customer_id
    HAVING SUM(i.total_final_cost) > 0  -- Éliminer les clients sans revenus
    ORDER BY total_spent DESC
    LIMIT %s
    """
    return execute_query(query, (limit,))

@st.cache_data(ttl=300)
def get_daily_revenue(days=30):
    """Revenus quotidiens"""
    query = """
    SELECT 
        DATE(billing_period) as date,
        SUM(total_final_cost) as daily_revenue,
        COUNT(*) as daily_invoices
    FROM invoices
    WHERE billing_period >= CURRENT_DATE - INTERVAL '%s days'
    GROUP BY DATE(billing_period)
    ORDER BY date
    """
    return execute_query(query % days)

@st.cache_data(ttl=300)
def get_service_breakdown():
    """Détail par service"""
    query = """
    SELECT 
        r.record_type,
        COUNT(*) as call_count,
        SUM(r.final_cost) as total_revenue,
        AVG(r.final_cost) as avg_cost,
        SUM(CASE WHEN r.record_type = 'voice' THEN r.duration_sec ELSE 0 END) as total_duration,
        SUM(CASE WHEN r.record_type = 'data' THEN r.data_volume_mb ELSE 0 END) as total_data_mb
    FROM rated_cdr r
    WHERE r.rating_status = 'rated'
        AND r.timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY r.record_type
    """
    return execute_query(query)

# Mapping des villes vers les régions
REGION_MAPPING = {
    # Région de Casablanca-Settat
    'CASABLANCA': 'Casablanca-Settat',
    'Casablanca': 'Casablanca-Settat',
    'SETTAT': 'Casablanca-Settat',
    'Settat': 'Casablanca-Settat',
    'MOHAMMEDIA': 'Casablanca-Settat',
    'Mohammedia': 'Casablanca-Settat',
    'EL JADIDA': 'Casablanca-Settat',
    'El Jadida': 'Casablanca-Settat',
    'BERRECHID': 'Casablanca-Settat',
    'Berrechid': 'Casablanca-Settat',
    
    # Région de Rabat-Salé-Kénitra
    'RABAT': 'Rabat-Salé-Kénitra',
    'Rabat': 'Rabat-Salé-Kénitra',
    'SALE': 'Rabat-Salé-Kénitra',
    'Salé': 'Rabat-Salé-Kénitra',
    'KENITRA': 'Rabat-Salé-Kénitra',
    'Kénitra': 'Rabat-Salé-Kénitra',
    'TEMARA': 'Rabat-Salé-Kénitra',
    'Témara': 'Rabat-Salé-Kénitra',
    'SKHIRAT': 'Rabat-Salé-Kénitra',
    'Skhirat': 'Rabat-Salé-Kénitra',
    
    # Région de Marrakech-Safi
    'MARRAKECH': 'Marrakech-Safi',
    'Marrakech': 'Marrakech-Safi',
    'SAFI': 'Marrakech-Safi',
    'Safi': 'Marrakech-Safi',
    'ESSAOUIRA': 'Marrakech-Safi',
    'Essaouira': 'Marrakech-Safi',
    'EL KELAA DES SRAGHNA': 'Marrakech-Safi',
    'El Kelaa des Sraghna': 'Marrakech-Safi',
    
    # Région de Fès-Meknès
    'FES': 'Fès-Meknès',
    'Fès': 'Fès-Meknès',
    'MEKNES': 'Fès-Meknès',
    'Meknès': 'Fès-Meknès',
    'TAZA': 'Fès-Meknès',
    'Taza': 'Fès-Meknès',
    'SEFROU': 'Fès-Meknès',
    'Sefrou': 'Fès-Meknès',
    
    # Région de Tanger-Tétouan-Al Hoceïma
    'TANGER': 'Tanger-Tétouan-Al Hoceïma',
    'Tanger': 'Tanger-Tétouan-Al Hoceïma',
    'TETOUAN': 'Tanger-Tétouan-Al Hoceïma',
    'Tétouan': 'Tanger-Tétouan-Al Hoceïma',
    'AL HOCEIMA': 'Tanger-Tétouan-Al Hoceïma',
    'Al Hoceïma': 'Tanger-Tétouan-Al Hoceïma',
    'CHEFCHAOUEN': 'Tanger-Tétouan-Al Hoceïma',
    'Chefchaouen': 'Tanger-Tétouan-Al Hoceïma',
    
    # Région de l'Oriental
    'OUJDA': 'Oriental',
    'Oujda': 'Oriental',
    'NADOR': 'Oriental',
    'Nador': 'Oriental',
    'BERKANE': 'Oriental',
    'Berkane': 'Oriental',
    'TAOURIRT': 'Oriental',
    'Taourirt': 'Oriental',
    
    # Région de Souss-Massa
    'AGADIR': 'Souss-Massa',
    'Agadir': 'Souss-Massa',
    'TIZNIT': 'Souss-Massa',
    'Tiznit': 'Souss-Massa',
    'TAROUDANT': 'Souss-Massa',
    'Taroudant': 'Souss-Massa',
    'INEZGANE': 'Souss-Massa',
    'Inezgane': 'Souss-Massa',
    
    # Région de Béni Mellal-Khénifra
    'BENI MELLAL': 'Béni Mellal-Khénifra',
    'Béni Mellal': 'Béni Mellal-Khénifra',
    'KHENIFRA': 'Béni Mellal-Khénifra',
    'Khénifra': 'Béni Mellal-Khénifra',
    'KHOURIBGA': 'Béni Mellal-Khénifra',
    'Khouribga': 'Béni Mellal-Khénifra',
    
    # Région de Drâa-Tafilalet
    'OUARZAZATE': 'Drâa-Tafilalet',
    'Ouarzazate': 'Drâa-Tafilalet',
    'ERRACHIDIA': 'Drâa-Tafilalet',
    'Errachidia': 'Drâa-Tafilalet',
    'ZAGORA': 'Drâa-Tafilalet',
    'Zagora': 'Drâa-Tafilalet',
    
    # Région de Laâyoune-Sakia El Hamra
    'LAAYOUNE': 'Laâyoune-Sakia El Hamra',
    'Laâyoune': 'Laâyoune-Sakia El Hamra',
    'BOUJDOUR': 'Laâyoune-Sakia El Hamra',
    'Boujdour': 'Laâyoune-Sakia El Hamra',
    
    # Région de Dakhla-Oued Ed-Dahab
    'DAKHLA': 'Dakhla-Oued Ed-Dahab',
    'Dakhla': 'Dakhla-Oued Ed-Dahab',
    
    # Région de Guelmim-Oued Noun
    'GUELMIM': 'Guelmim-Oued Noun',
    'Guelmim': 'Guelmim-Oued Noun',
    'TAN-TAN': 'Guelmim-Oued Noun',
    'Tan-Tan': 'Guelmim-Oued Noun'
}

def map_city_to_region(city):
    """Mapper une ville vers sa région administrative"""
    if pd.isna(city) or city is None:
        return 'Non définie'
    
    # Nettoyer la chaîne et la convertir en string
    city_clean = str(city).strip()
    
    # Essayer d'abord avec la chaîne telle qu'elle est
    if city_clean in REGION_MAPPING:
        return REGION_MAPPING[city_clean]
    
    # Essayer avec la première lettre en majuscule
    city_title = city_clean.title()
    if city_title in REGION_MAPPING:
        return REGION_MAPPING[city_title]
    
    # Essayer en majuscules
    city_upper = city_clean.upper()
    if city_upper in REGION_MAPPING:
        return REGION_MAPPING[city_upper]
    
    # Si aucune correspondance trouvée
    return 'Autres'

@st.cache_data(ttl=300)
def get_revenue_by_region():
    """Récupérer les revenus par région"""
    query = """
    SELECT 
        c.region as city,
        COUNT(DISTINCT i.customer_id) as customer_count,
        COUNT(i.invoice_id) as invoice_count,
        SUM(i.total_final_cost) as total_revenue,
        AVG(i.total_final_cost) as avg_revenue_per_invoice,
        SUM(i.total_base_cost) as total_base_cost,
        SUM(i.total_discount) as total_discount,
        SUM(i.tax_applied) as total_tax
    FROM invoices i
    JOIN customers c ON i.customer_id = c.customer_id
    WHERE i.billing_period >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY c.region
    ORDER BY total_revenue DESC
    """
    df = execute_query(query)
    
    if not df.empty:
        # Mapper les villes vers les régions
        df['region'] = df['city'].apply(map_city_to_region)
        
        # Regrouper par région
        region_stats = df.groupby('region').agg({
            'customer_count': 'sum',
            'invoice_count': 'sum',
            'total_revenue': 'sum',
            'total_base_cost': 'sum',
            'total_discount': 'sum',
            'total_tax': 'sum'
        }).reset_index()
        
        # Calculer les métriques dérivées
        region_stats['avg_revenue_per_customer'] = region_stats['total_revenue'] / region_stats['customer_count']
        region_stats['revenue_share'] = (region_stats['total_revenue'] / region_stats['total_revenue'].sum()) * 100
        region_stats['discount_rate'] = (region_stats['total_discount'] / region_stats['total_base_cost']) * 100
        
        return region_stats.sort_values('total_revenue', ascending=False)
    
    return df

@st.cache_data(ttl=300)
def get_monthly_revenue_by_region():
    """Récupérer l'évolution mensuelle des revenus par région"""
    query = """
    SELECT 
        DATE_TRUNC('month', i.billing_period) as month,
        c.region as city,
        SUM(i.total_final_cost) as monthly_revenue,
        COUNT(i.invoice_id) as monthly_invoices,
        COUNT(DISTINCT i.customer_id) as active_customers
    FROM invoices i
    JOIN customers c ON i.customer_id = c.customer_id
    WHERE i.billing_period >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY DATE_TRUNC('month', i.billing_period), c.region
    ORDER BY month, monthly_revenue DESC
    """
    df = execute_query(query)
    
    if not df.empty:
        # Mapper les villes vers les régions
        df['region'] = df['city'].apply(map_city_to_region)
        
        # Regrouper par mois et région
        monthly_region_stats = df.groupby(['month', 'region']).agg({
            'monthly_revenue': 'sum',
            'monthly_invoices': 'sum',
            'active_customers': 'sum'
        }).reset_index()
        
        # Convertir le mois en datetime
        monthly_region_stats['month'] = pd.to_datetime(monthly_region_stats['month'])
        
        return monthly_region_stats
    
    return df

@st.cache_data(ttl=300)
def get_top_regions_performance():
    """Récupérer les performances des top régions avec plus de détails"""
    query = """
    SELECT 
        c.region as city,
        COUNT(DISTINCT i.customer_id) as total_customers,
        SUM(i.total_final_cost) as total_revenue,
        AVG(i.total_final_cost) as avg_invoice_amount,
        MAX(i.billing_period) as last_billing_period,
        
        -- Revenus par service
        SUM(CASE WHEN r.record_type = 'voice' THEN r.final_cost ELSE 0 END) as voice_revenue,
        SUM(CASE WHEN r.record_type = 'sms' THEN r.final_cost ELSE 0 END) as sms_revenue,
        SUM(CASE WHEN r.record_type = 'data' THEN r.final_cost ELSE 0 END) as data_revenue,
        
        -- Volumes par service
        SUM(CASE WHEN r.record_type = 'voice' THEN r.duration_sec ELSE 0 END) / 60.0 as voice_minutes,
        COUNT(CASE WHEN r.record_type = 'sms' THEN 1 END) as sms_count,
        SUM(CASE WHEN r.record_type = 'data' THEN r.data_volume_mb ELSE 0 END) as data_mb
        
    FROM invoices i
    JOIN customers c ON i.customer_id = c.customer_id
    LEFT JOIN rated_cdr r ON r.customer_id = i.customer_id 
        AND date_trunc('month', r.timestamp) = i.billing_period
        AND r.rating_status = 'rated'
    WHERE i.billing_period >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY c.region
    HAVING SUM(i.total_final_cost) > 0
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    df = execute_query(query)
    
    if not df.empty:
        # Mapper les villes vers les régions
        df['region'] = df['city'].apply(map_city_to_region)
        
        # Regrouper par région
        region_performance = df.groupby('region').agg({
            'total_customers': 'sum',
            'total_revenue': 'sum',
            'voice_revenue': 'sum',
            'sms_revenue': 'sum',
            'data_revenue': 'sum',
            'voice_minutes': 'sum',
            'sms_count': 'sum',
            'data_mb': 'sum'
        }).reset_index()
        
        # Calculer les métriques dérivées
        region_performance['arpu'] = region_performance['total_revenue'] / region_performance['total_customers']
        region_performance['voice_share'] = (region_performance['voice_revenue'] / region_performance['total_revenue']) * 100
        region_performance['sms_share'] = (region_performance['sms_revenue'] / region_performance['total_revenue']) * 100
        region_performance['data_share'] = (region_performance['data_revenue'] / region_performance['total_revenue']) * 100
        
        return region_performance.sort_values('total_revenue', ascending=False)
    
    return df

def get_all_invoices():
    """Récupérer toutes les factures"""
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame()
            
        query = """
        SELECT 
            i.invoice_id,
            i.customer_id,
            c.customer_name,
            i.billing_period,
            i.total_base_cost,
            i.total_discount,
            i.tax_applied,
            i.total_final_cost,
            c.rate_plan_id
        FROM invoices i
        LEFT JOIN customers c ON i.customer_id = c.customer_id
        ORDER BY i.billing_period DESC, i.customer_id
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Convertir billing_period en datetime si ce n'est pas déjà fait
        if not df.empty and 'billing_period' in df.columns:
            df['billing_period'] = pd.to_datetime(df['billing_period'])
            
        return df
    except Exception as e:
        st.error(f"Erreur lors de la récupération des factures: {e}")
        return pd.DataFrame()

def get_invoice_details(customer_id, billing_period):
    """Récupérer les détails d'une facture spécifique"""
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame(), pd.DataFrame()
        
        # Informations de base de la facture
        invoice_query = """
        SELECT 
            i.*,
            c.customer_name,
            c.rate_plan_id,
            c.is_student,
            c.activation_date,
            c.region
        FROM invoices i
        LEFT JOIN customers c ON i.customer_id = c.customer_id
        WHERE i.customer_id = %s AND i.billing_period = %s
        """
        
        # Détails des usages facturés
        usage_query = """
        SELECT 
            record_type,
            COUNT(*) as usage_count,
            SUM(duration_sec) as total_duration_sec,
            SUM(data_volume_mb) as total_data_mb,
            SUM(base_cost) as total_base_cost,
            SUM(final_cost) as total_final_cost,
            AVG(unit_price) as avg_unit_price
        FROM rated_cdr
        WHERE customer_id = %s 
        AND date_trunc('month', timestamp) = date_trunc('month', %s::date)
        AND rating_status = 'rated'
        GROUP BY record_type
        """
        
        invoice_df = pd.read_sql(invoice_query, conn, params=[customer_id, billing_period])
        usage_df = pd.read_sql(usage_query, conn, params=[customer_id, billing_period])
        
        conn.close()
        return invoice_df, usage_df
    except Exception as e:
        st.error(f"Erreur lors de la récupération des détails: {e}")
        return pd.DataFrame(), pd.DataFrame()

def get_billing_statistics():
    """Récupérer les statistiques de facturation"""
    try:
        conn = get_connection()
        if conn is None:
            return pd.DataFrame(), pd.DataFrame()
        
        stats_query = """
        SELECT 
            COUNT(*) as total_invoices,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(total_final_cost) as total_revenue,
            AVG(total_final_cost) as avg_invoice_amount,
            MAX(billing_period) as latest_period,
            MIN(billing_period) as earliest_period
        FROM invoices
        """
        
        monthly_revenue_query = """
        SELECT 
            billing_period,
            COUNT(*) as invoice_count,
            SUM(total_final_cost) as monthly_revenue,
            AVG(total_final_cost) as avg_amount
        FROM invoices
        GROUP BY billing_period
        ORDER BY billing_period DESC
        LIMIT 12
        """
        
        stats_df = pd.read_sql(stats_query, conn)
        monthly_df = pd.read_sql(monthly_revenue_query, conn)
        
        # Fermer la connexion seulement après toutes les requêtes
        conn.close()
        
        # Convertir billing_period en datetime pour monthly_df
        if not monthly_df.empty and 'billing_period' in monthly_df.columns:
            monthly_df['billing_period'] = pd.to_datetime(monthly_df['billing_period'])
        
        return stats_df, monthly_df
    except Exception as e:
        st.error(f"Erreur lors de la récupération des statistiques: {e}")
        return pd.DataFrame(), pd.DataFrame()