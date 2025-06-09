import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
from utils.database import *
from utils.charts import *
from config import DASHBOARD_CONFIG, COLORS

# Configuration de la page
st.set_page_config(
    page_title=DASHBOARD_CONFIG['title'],
    page_icon="📊",
    layout=DASHBOARD_CONFIG['layout'],
    initial_sidebar_state="expanded"
)

# CSS personnalisé pour un style moderne
st.markdown("""
<style>
    .main > div {
        padding-top: 2rem;
    }
    
    .metric-card {
        background: linear-gradient(45deg, #1e3a8a, #3b82f6);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #60a5fa;
    }
    
    .metric-label {
        font-size: 0.9rem;
        color: #cbd5e1;
    }
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #1e293b, #334155);
    }
    
    .stSelectbox > div > div {
        background-color: #334155;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

def main():
    # Titre principal avec emoji et style
    st.markdown("""
    # Telecom Analytics Dashboard
    ### Tableau de bord analytique pour les télécommunications
    """)
    
    # Sidebar pour les filtres
    with st.sidebar:
        st.markdown("## 🔧 Filtres et Options")
        
        # Sélecteur de période
        period_options = ["7 jours", "30 jours", "3 mois", "6 mois", "1 an"]
        selected_period = st.selectbox("📅 Période d'analyse", period_options, index=1)
        
        # Sélecteur de service
        service_options = ["Tous", "voice", "data", "sms"]
        selected_service = st.selectbox("📱 Type de service", service_options)
        
        # Bouton de rafraîchissement
        if st.button("🔄 Actualiser les données", type="primary"):
            st.cache_data.clear()
            st.rerun()
        
        # Informations système
        st.markdown("---")
        st.markdown("### ℹ️ Informations")
        st.info(f"Dernière mise à jour: {datetime.now().strftime('%H:%M:%S')}")
    
    # Métriques principales en haut
    st.markdown("### Vue d'ensemble")
    
    # Récupérer les données
    revenue_data = get_revenue_metrics()
    customer_data = get_customer_metrics()
    usage_data = get_usage_by_service()
    
    if not revenue_data.empty and not customer_data.empty:
        # Calculs des métriques
        total_revenue = revenue_data['total_revenue'].sum()
        total_invoices = revenue_data['total_invoices'].sum()
        avg_invoice = total_revenue / total_invoices if total_invoices > 0 else 0
        
        customer_metrics = customer_data.iloc[0]
        active_customers = customer_metrics['active_customers']
        
        # Affichage des métriques principales
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
            <div class="metric-card">
                <div class="metric-label">💰 Revenus Totaux</div>
                <div class="metric-value">{:,.0f} MAD</div>
            </div>
            """.format(total_revenue), unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-card">
                <div class="metric-label">👥 Clients Actifs</div>
                <div class="metric-value">{:,}</div>
            </div>
            """.format(active_customers), unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="metric-card">
                <div class="metric-label">📋 Factures</div>
                <div class="metric-value">{:,}</div>
            </div>
            """.format(int(total_invoices)), unsafe_allow_html=True)
        
        with col4:
            st.markdown("""
            <div class="metric-card">
                <div class="metric-label">💳 Facture Moyenne</div>
                <div class="metric-value">{:.2f} MAD</div>
            </div>
            """.format(avg_invoice), unsafe_allow_html=True)
    
    # Graphiques principaux
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if not revenue_data.empty:
            fig_revenue = create_revenue_chart(revenue_data)
            st.plotly_chart(fig_revenue, use_container_width=True)
        else:
            st.warning("Aucune donnée de revenus disponible")
    
    with col2:
        if not usage_data.empty:
            fig_services = create_service_pie_chart(usage_data)
            st.plotly_chart(fig_services, use_container_width=True)
        else:
            st.warning("Aucune donnée d'usage disponible")
    
    # Tableau des données récentes
    st.markdown("### Données Détaillées")
    
    tab1, tab2, tab3 = st.tabs(["💰 Revenus", "👥 Clients", "📱 Usage"])
    
    with tab1:
        if not revenue_data.empty:
            st.dataframe(
                revenue_data.style.format({
                    'total_revenue': '{:,.2f} MAD',
                    'avg_invoice': '{:.2f} MAD'
                }),
                use_container_width=True
            )
        else:
            st.info("Aucune donnée de revenus à afficher")
    
    with tab2:
        if not customer_data.empty:
            st.dataframe(customer_data, use_container_width=True)
        else:
            st.info("Aucune donnée client à afficher")
    
    with tab3:
        if not usage_data.empty:
            st.dataframe(
                usage_data.style.format({
                    'total_cost': '{:,.2f} MAD',
                    'avg_cost': '{:.2f} MAD'
                }),
                use_container_width=True
            )
        else:
            st.info("Aucune donnée d'usage à afficher")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #64748b; padding: 1rem;'>
        📊 Telecom Analytics Dashboard - Projet Big Data 2024
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()