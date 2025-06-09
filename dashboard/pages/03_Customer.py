import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
from utils.database import *
from utils.charts import *
from config import COLORS

st.set_page_config(page_title="Customer Analytics", page_icon="👥", layout="wide")

# CSS moderne unifié - palette cohérente
st.markdown("""
<style>
    .customer-segment {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 8px 32px rgba(30, 41, 59, 0.4);
        border: 1px solid rgba(148, 163, 184, 0.1);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        min-height: 140px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }
    
    .customer-segment:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 40px rgba(30, 41, 59, 0.6);
    }
    
    .customer-segment h3 {
        margin: 0 0 0.5rem 0;
        font-size: 1rem;
        font-weight: 600;
        opacity: 0.9;
    }
    
    .customer-segment h2 {
        margin: 0 0 0.5rem 0;
        font-size: 2.2rem;
        font-weight: 700;
        color: #60a5fa;
    }
    
    .customer-segment p {
        margin: 0;
        font-size: 0.875rem;
        opacity: 0.8;
    }
    
    .chart-container {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border-radius: 15px;
        padding: 1.5rem;
        border: 1px solid rgba(148, 163, 184, 0.1);
        box-shadow: 0 8px 32px rgba(30, 41, 59, 0.3);
        margin: 1rem 0;
    }
    
    .chart-container h3 {
        color: #f8fafc;
        margin-bottom: 1rem;
        font-weight: 600;
    }
    
    /* Uniformisation des colonnes */
    .stColumn > div {
        height: 100%;
    }
    
    /* Style pour les métriques */
    .metric-highlight {
        color: #60a5fa !important;
        font-weight: 700;
    }
</style>
""", unsafe_allow_html=True)

def safe_qcut(series, q, labels=None):
    """Fonction pour gérer les cas où qcut échoue avec des valeurs dupliquées"""
    try:
        return pd.qcut(series, q, labels=labels, duplicates='drop')
    except ValueError:
        # Si toutes les valeurs sont identiques, assigner le score du milieu
        if labels:
            return pd.Series([labels[len(labels)//2]] * len(series), index=series.index)
        else:
            return pd.Series([q//2] * len(series), index=series.index)

# def get_morocco_regions_data():
#     """Simulation des données par région du Maroc - à remplacer par vos vraies données"""
#     # Ceci est un exemple - remplacez par votre vraie requête de base de données
#     regions_data = pd.DataFrame({
#         'region': ['Casablanca-Settat', 'Rabat-Salé-Kénitra', 'Fès-Meknès', 'Marrakech-Safi', 
#                   'Tanger-Tétouan-Al Hoceïma', 'Souss-Massa', 'Oriental', 'Béni Mellal-Khénifra',
#                   'Draâ-Tafilalet', 'Laâyoune-Sakia El Hamra', 'Dakhla-Oued Ed-Dahab', 'Guelmim-Oued Noun'],
#         'total_revenue': [45000000, 28000000, 22000000, 35000000, 
#                          18000000, 15000000, 12000000, 8000000,
#                          6000000, 4000000, 2500000, 3000000],
#         'customer_count': [1250, 780, 650, 920, 
#                           480, 420, 350, 280,
#                           180, 120, 80, 100]
#     })
#     return regions_data

@st.cache_data(ttl=600)  # Cache pendant 10 minutes
def get_cached_customer_data():
    """Récupérer les données clients avec cache"""
    return get_customer_metrics()

@st.cache_data(ttl=600)  # Cache pendant 10 minutes  
def get_cached_top_customers(limit=20):
    """Récupérer le top des clients avec cache"""
    return get_top_customers(limit)

@st.cache_data(ttl=600)
def calculate_rfm_segments(top_customers_df):
    """Calculer les segments RFM avec cache"""
    if top_customers_df.empty or len(top_customers_df) <= 3:
        return top_customers_df
    
    df = top_customers_df.copy()
    
    # Calcul des scores RFM
    df['frequency_score'] = safe_qcut(df['total_invoices'], 3, labels=[1, 2, 3])
    df['monetary_score'] = safe_qcut(df['total_spent'], 3, labels=[1, 2, 3])
    
    # Score RFM combiné
    df['rfm_score'] = (
        pd.to_numeric(df['frequency_score'], errors='coerce').fillna(2) + 
        pd.to_numeric(df['monetary_score'], errors='coerce').fillna(2)
    )
    
    # Segmentation
    def rfm_segment(score):
        if score >= 5:
            return "🌟 Champions"
        elif score >= 4:
            return "💎 Loyaux"
        elif score >= 3:
            return "📈 Potentiel"
        else:
            return "🚨 À risque"
    
    df['segment'] = df['rfm_score'].apply(rfm_segment)
    return df

def main():
    st.markdown("#  Customer Analytics")
    st.markdown("### Analyse comportementale et segmentation client")
    
    # Sidebar épuré
    with st.sidebar:
        st.markdown("###  Segmentation")
        
        segment_type = st.selectbox(
            "Type de segmentation",
            ["Revenus", "Usage", "Ancienneté"],
            index=0
        )
        
        show_churn = st.checkbox("🚨 Analyse churn", value=False)
        show_lifetime = st.checkbox("💎 Customer Lifetime Value", value=False)
        show_regions = st.checkbox("🗺️ Analyse par région", value=False)  # Désactivé par défaut
        
        st.markdown("---")
        st.markdown("### 📊 Filtres")
        
        customer_status = st.multiselect(
            "Statut client",
            ["active", "inactive", "suspended"],
            default=["active"]
        )
    
    # Chargement des données avec spinner et cache
    with st.spinner("Chargement des données clients..."):
        customer_data = get_cached_customer_data()
        top_customers = get_cached_top_customers(20)
    
    if not customer_data.empty:
        metrics = customer_data.iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div class="customer-segment">
                <h3>👥 Total Clients</h3>
                <h2>{metrics['total_customers']:,}</h2>
                <p>Base client totale</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            activation_rate = (metrics['active_customers'] / metrics['total_customers'] * 100) if metrics['total_customers'] > 0 else 0
            st.markdown(f"""
            <div class="customer-segment">
                <h3>✅ Clients Actifs</h3>
                <h2>{metrics['active_customers']:,}</h2>
                <p>{activation_rate:.1f}% du total</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            student_rate = (metrics['student_customers'] / metrics['total_customers'] * 100) if metrics['total_customers'] > 0 else 0
            st.markdown(f"""
            <div class="customer-segment">
                <h3>🎓 Étudiants</h3>
                <h2>{metrics['student_customers']:,}</h2>
                <p>{student_rate:.1f}% du total</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            avg_age = metrics['avg_customer_age_years'] or 0
            st.markdown(f"""
            <div class="customer-segment">
                <h3>📅 Ancienneté Moyenne</h3>
                <h2>{avg_age:.1f}</h2>
                <p>années d'ancienneté</p>
            </div>
            """, unsafe_allow_html=True)

    # Graphiques de segmentation
    st.markdown("---")
    st.markdown("###  Segmentation Clients")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        if not top_customers.empty:
            # Top 10 clients par revenus - IDs complets
            top_10 = top_customers.head(10)
            
            # Afficher les ID des clients pour plus d'informations
            fig = px.bar(
                top_10,
                x='total_spent',
                y='customer_id',  # Afficher les ID des clients
                orientation='h',
                title="💰 Top 10 Clients par Revenus",
                labels={'total_spent': 'Revenus (MAD)', 'customer_id': 'ID Client'},
                color='total_spent',
                color_continuous_scale='blues'
            )
            
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#f8fafc',
                yaxis={'categoryorder': 'total ascending'},
                height=400,  # Réduction de la hauteur
                showlegend=False,
                coloraxis_showscale=False  # Masquer l'échelle de couleur pour simplifier
            )
            
            st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            # Distribution des revenus - Version simplifiée
            fig = px.histogram(
                top_customers,
                x='total_spent',
                nbins=10,  # Réduction du nombre de bins
                title="📈 Distribution des Revenus",
                labels={'total_spent': 'Revenus (MAD)', 'count': 'Nombre'},
                color_discrete_sequence=['#60a5fa']
            )
            
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#f8fafc',
                height=400,
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
    
    # Tableau détaillé des top clients
    if not top_customers.empty:
        st.markdown("---")
        st.markdown("###  📋 Détails des Top Clients")
        
        # Formater les données pour l'affichage
        display_df = top_customers.head(10).copy()
        display_df['total_spent'] = display_df['total_spent'].apply(lambda x: f"{x:,.0f} MAD")
        display_df['avg_invoice'] = display_df['avg_invoice'].apply(lambda x: f"{x:,.0f} MAD")
        
        # Renommer les colonnes pour l'affichage
        display_df = display_df.rename(columns={
            'customer_id': 'ID Client',
            'total_spent': 'Revenus Total',
            'total_invoices': 'Nb Factures',
            'avg_invoice': 'Facture Moyenne'
        })
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.warning("Aucune donnée client disponible")
    
    # Analyse RFM avec style unifié (seulement si demandé)
    if segment_type == "Revenus" and not top_customers.empty and len(top_customers) > 3:
        st.markdown("---")
        st.markdown("###  Analyse RFM")
        
        with st.spinner("Calcul des segments RFM..."):
            top_customers_rfm = calculate_rfm_segments(top_customers)
        
        if 'segment' in top_customers_rfm.columns:
            # Graphiques avec palette unifiée - Version optimisée
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                segment_counts = top_customers_rfm['segment'].value_counts()
                
                # Couleurs simplifiées
                colors = ['#60a5fa', '#34d399', '#fbbf24', '#f87171']
                
                fig = px.pie(
                    values=segment_counts.values,
                    names=segment_counts.index,
                    title="🎯 Segmentation RFM",
                    color_discrete_sequence=colors
                )
                
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#f8fafc',
                    height=350,  # Réduction de la hauteur
                    showlegend=True
                )
                
                st.plotly_chart(fig, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                # Version simplifiée du scatter plot
                color_map = {
                    "🌟 Champions": "#60a5fa",
                    "💎 Loyaux": "#34d399", 
                    "📈 Potentiel": "#fbbf24",
                    "🚨 À risque": "#f87171"
                }
                
                fig = px.scatter(
                    top_customers_rfm.head(50),  # Limiter à 50 points pour les performances
                    x='total_invoices',
                    y='total_spent',
                    color='segment',
                    title="💰 Fréquence vs Montant",
                    labels={
                        'total_invoices': 'Nb factures',
                        'total_spent': 'Total (MAD)'
                    },
                    color_discrete_map=color_map
                )
                
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#f8fafc',
                    height=350
                )
                
                st.plotly_chart(fig, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Tableau récapitulatif simplifié
            st.markdown("###  Résumé par Segment")
            
            segment_stats = top_customers_rfm.groupby('segment').agg({
                'customer_id': 'count',
                'total_spent': ['mean', 'sum'],
                'total_invoices': 'mean'
            }).round(2)
            
            segment_stats.columns = ['Nombre', 'Revenus Moyen', 'Revenus Total', 'Factures Moyennes']
            
            # Affichage simplifié du tableau
            st.dataframe(
                segment_stats.style.format({
                    'Revenus Moyen': '{:.0f} MAD',
                    'Revenus Total': '{:.0f} MAD'
                }),
                use_container_width=True
            )
    else:
        st.info("ℹ️ Sélectionnez 'Revenus' dans la segmentation pour voir l'analyse RFM")
    
    # Sections optionnelles (chargement à la demande)
    if show_regions and st.button("🗺️ Charger l'analyse par région"):
        st.markdown("---")
        st.markdown("###  Analyse par Région")
        with st.spinner("Chargement des données régionales..."):
            try:
                region_data = get_revenue_by_region()
                if not region_data.empty:
                    # Graphique simplifié par région
                    fig = px.bar(
                        region_data.head(8),  # Top 8 régions seulement
                        x='region',
                        y='total_revenue',
                        title="💰 Revenus par Région (Top 8)",
                        color='total_revenue',
                        color_continuous_scale='blues'
                    )
                    fig.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font_color='#f8fafc',
                        xaxis_tickangle=-45,
                        height=400
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Aucune donnée régionale disponible")
            except Exception as e:
                st.error(f"Erreur lors du chargement des données régionales: {e}")
    
    if show_churn:
        st.markdown("---")
        st.markdown("###  Analyse de Churn")
        st.info("🚧 Fonctionnalité en développement - Analyse des clients à risque de départ")
    
    if show_lifetime:
        st.markdown("---")
        st.markdown("###  Customer Lifetime Value")
        st.info("🚧 Fonctionnalité en développement - Calcul de la valeur vie client")

if __name__ == "__main__":
    main()