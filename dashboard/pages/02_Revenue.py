import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
from utils.database import (
    get_revenue_metrics, 
    get_daily_revenue, 
    get_customer_metrics, 
    get_service_breakdown,
    get_revenue_by_region,
    get_monthly_revenue_by_region,
    get_top_regions_performance
)
from utils.charts import *
from config import COLORS

st.set_page_config(
    page_title="Revenue Analytics", 
    page_icon="💰", 
    layout="wide"
)

# CSS pour cette page
st.markdown("""
<style>
    .revenue-kpi {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    }
    
    .growth-positive {
        color: #10b981;
        font-weight: bold;
    }
    
    .growth-negative {
        color: #ef4444;
        font-weight: bold;
    }
    
    .chart-container {
        background: rgba(255,255,255,0.05);
        border-radius: 15px;
        padding: 1rem;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255,255,255,0.1);
    }
</style>
""", unsafe_allow_html=True)

def main():
    st.markdown("# Revenue Analytics")
    st.markdown("### Analyse détaillée des revenus et performances financières")
    
    # Sidebar pour filtres avancés
    with st.sidebar:
        st.markdown("## 🎛️ Filtres Avancés")
        
        # Sélecteur de période
        date_range = st.date_input(
            "📅 Période d'analyse",
            value=(datetime.now() - timedelta(days=90), datetime.now()),
            max_value=datetime.now()
        )
        
        # Sélecteur de granularité
        granularity = st.selectbox(
            "📊 Granularité",
            ["Jour", "Semaine", "Mois", "Trimestre"]
        )
        
        # Sélecteur de métrique
        metric_type = st.selectbox(
            "📈 Métrique principale",
            ["Revenus", "ARPU", "Croissance", "Marge"]
        )
        
        # Export des données
        if st.button("📥 Exporter données", type="secondary"):
            st.success("Export en cours...")
    
    # KPIs Revenue en haut
    st.markdown("### KPIs Revenue")
    
    # Récupérer les données
    revenue_data = get_revenue_metrics()
    daily_revenue = get_daily_revenue(30)
    customer_data = get_customer_metrics()
    
    if not revenue_data.empty:
        # Calculs des métriques avancées
        current_month = revenue_data.iloc[-1] if len(revenue_data) > 0 else None
        prev_month = revenue_data.iloc[-2] if len(revenue_data) > 1 else None
        
        if current_month is not None and prev_month is not None:
            revenue_growth = ((current_month['total_revenue'] - prev_month['total_revenue']) / prev_month['total_revenue'] * 100)
            invoice_growth = ((current_month['total_invoices'] - prev_month['total_invoices']) / prev_month['total_invoices'] * 100)
        else:
            revenue_growth = 0
            invoice_growth = 0
        
        # Calcul ARPU
        total_customers = customer_data.iloc[0]['active_customers'] if not customer_data.empty else 1
        arpu = current_month['total_revenue'] / total_customers if current_month is not None else 0
        
        # Affichage des KPIs
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            growth_class = "growth-positive" if revenue_growth >= 0 else "growth-negative"
            st.markdown(f"""
            <div class="revenue-kpi">
                <h3>💰 Revenus Mensuel</h3>
                <h2>{current_month['total_revenue']:,.0f} MAD</h2>
                <p class="{growth_class}">{revenue_growth:+.1f}% vs mois précédent</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="revenue-kpi">
                <h3>📈 ARPU</h3>
                <h2>{arpu:.2f} MAD</h2>
                <p>Revenus par utilisateur actif</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            growth_class = "growth-positive" if invoice_growth >= 0 else "growth-negative"
            st.markdown(f"""
            <div class="revenue-kpi">
                <h3>📋 Factures</h3>
                <h2>{current_month['total_invoices']:,.0f}</h2>
                <p class="{growth_class}">{invoice_growth:+.1f}% vs mois précédent</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            total_revenue_ytd = revenue_data['total_revenue'].sum()
            st.markdown(f"""
            <div class="revenue-kpi">
                <h3>🎯 Total YTD</h3>
                <h2>{total_revenue_ytd:,.0f} MAD</h2>
                <p>Cumul année en cours</p>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Graphiques principaux
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        if not daily_revenue.empty:
            # Graphique des revenus quotidiens avec tendance
            fig = go.Figure()
            
            # Ligne des revenus
            fig.add_trace(go.Scatter(
                x=daily_revenue['date'],
                y=daily_revenue['daily_revenue'],
                mode='lines+markers',
                name='Revenus quotidiens',
                line=dict(color=COLORS['primary'], width=3),
                marker=dict(size=6)
            ))
            
            # Moyenne mobile 7 jours
            daily_revenue['ma7'] = daily_revenue['daily_revenue'].rolling(7).mean()
            fig.add_trace(go.Scatter(
                x=daily_revenue['date'],
                y=daily_revenue['ma7'],
                mode='lines',
                name='Moyenne 7j',
                line=dict(color=COLORS['secondary'], width=2, dash='dash')
            ))
            
            fig.update_layout(
                title="📈 Évolution des Revenus Quotidiens",
                xaxis_title="Date",
                yaxis_title="Revenus (MAD)",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white',
                showlegend=True,
                legend=dict(
                    bgcolor="rgba(0,0,0,0.5)",
                    bordercolor="rgba(255,255,255,0.2)",
                    borderwidth=1
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        if not revenue_data.empty:
            # Graphique en barres avec objectifs
            fig = go.Figure()
            
            # Barres des revenus réels
            fig.add_trace(go.Bar(
                x=revenue_data['month'],
                y=revenue_data['total_revenue'],
                name='Revenus réels',
                marker_color=COLORS['primary'],
                opacity=0.8
            ))
            
            # Ligne d'objectif (exemple: 10% de croissance mensuelle)
            if len(revenue_data) > 1:
                baseline = revenue_data.iloc[0]['total_revenue']
                objectives = [baseline * (1.1 ** i) for i in range(len(revenue_data))]
                
                fig.add_trace(go.Scatter(
                    x=revenue_data['month'],
                    y=objectives,
                    mode='lines+markers',
                    name='Objectif (+10%/mois)',
                    line=dict(color=COLORS['warning'], width=3, dash='dot'),
                    marker=dict(size=8, symbol='diamond')
                ))
            
            fig.update_layout(
                title="🎯 Revenus vs Objectifs",
                xaxis_title="Mois",
                yaxis_title="Revenus (MAD)",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white',
                showlegend=True,
                legend=dict(
                    bgcolor="rgba(0,0,0,0.5)",
                    bordercolor="rgba(255,255,255,0.2)",
                    borderwidth=1
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Analyse par service
    st.markdown("###  Analyse par Service")
    
    service_data = get_service_breakdown()
    if not service_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Revenus par service - Donut chart
            fig = go.Figure(data=[go.Pie(
                labels=service_data['record_type'],
                values=service_data['total_revenue'],
                hole=0.6,
                marker_colors=[COLORS['primary'], COLORS['secondary'], COLORS['success']]
            )])
            
            fig.update_layout(
                title="💰 Revenus par Service",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white',
                annotations=[dict(text='Revenus', x=0.5, y=0.5, font_size=20, showarrow=False)]
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Revenus par région - Graphique en barres horizontal
            region_data = get_revenue_by_region()
            if not region_data.empty:
                # Limiter aux top 8 régions pour la lisibilité
                top_regions = region_data.head(8)
                
                fig = go.Figure()
                
                fig.add_trace(go.Bar(
                    x=top_regions['total_revenue'],
                    y=top_regions['region'],
                    orientation='h',
                    marker=dict(
                        color=top_regions['total_revenue'],
                        colorscale='Viridis',
                        showscale=True,
                        colorbar=dict(title="Revenus (MAD)")
                    ),
                    text=[f"{rev:,.0f} MAD" for rev in top_regions['total_revenue']],
                    textposition="auto",
                    name="Revenus par région"
                ))
                
                fig.update_layout(
                    title="🗺️ Top Régions par Revenus",
                    xaxis_title="Revenus totaux (MAD)",
                    yaxis_title="Région",
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='white',
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Aucune donnée régionale disponible")
    
    # Tableau détaillé
    st.markdown("### Détails par Service")
    
    if not service_data.empty:
        # Formatage du tableau
        styled_df = service_data.style.format({
            'total_revenue': '{:,.2f} MAD',
            'avg_cost': '{:.4f} MAD',
            'call_count': '{:,}',
            'total_duration': '{:,.0f} sec',
            'total_data_mb': '{:,.2f} MB'
        }).background_gradient(subset=['total_revenue'], cmap='Blues')
        
        st.dataframe(styled_df, use_container_width=True, height=300)

    # Analyse régionale détaillée
    st.markdown("### 🗺️ Analyse Régionale")
    
    region_data = get_revenue_by_region()
    monthly_region_data = get_monthly_revenue_by_region()
    
    if not region_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Graphique en secteurs des revenus par région
            top_regions = region_data.head(6)  # Top 6 régions
            others_revenue = region_data.iloc[6:]['total_revenue'].sum() if len(region_data) > 6 else 0
            
            if others_revenue > 0:
                plot_data = pd.concat([
                    top_regions[['region', 'total_revenue']],
                    pd.DataFrame({'region': ['Autres'], 'total_revenue': [others_revenue]})
                ])
            else:
                plot_data = top_regions[['region', 'total_revenue']]
            
            fig = go.Figure(data=[go.Pie(
                labels=plot_data['region'],
                values=plot_data['total_revenue'],
                hole=0.4,
                marker_colors=px.colors.qualitative.Set3[:len(plot_data)]
            )])
            
            fig.update_layout(
                title="💰 Répartition des Revenus par Région",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white',
                annotations=[dict(text='Revenus<br>Régionaux', x=0.5, y=0.5, font_size=16, showarrow=False)]
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # ARPU par région
            region_data['arpu'] = region_data['avg_revenue_per_customer']
            top_arpu_regions = region_data.nlargest(8, 'arpu')
            
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                y=top_arpu_regions['region'],
                x=top_arpu_regions['arpu'],
                orientation='h',
                marker=dict(
                    color=top_arpu_regions['arpu'],
                    colorscale='RdYlBu_r',
                    showscale=True,
                    colorbar=dict(title="ARPU (MAD)")
                ),
                text=[f"{arpu:.2f}" for arpu in top_arpu_regions['arpu']],
                textposition="auto"
            ))
            
            fig.update_layout(
                title="📊 ARPU par Région",
                xaxis_title="ARPU (MAD)",
                yaxis_title="Région",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white',
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Évolution mensuelle par région (pour les top 5 régions)
        if not monthly_region_data.empty:
            st.markdown("#### 📈 Évolution Mensuelle des Top Régions")
            
            top_5_regions = region_data.head(5)['region'].tolist()
            filtered_monthly = monthly_region_data[monthly_region_data['region'].isin(top_5_regions)]
            
            fig = go.Figure()
            
            colors = px.colors.qualitative.Set1[:5]
            for i, region in enumerate(top_5_regions):
                region_monthly = filtered_monthly[filtered_monthly['region'] == region]
                if not region_monthly.empty:
                    fig.add_trace(go.Scatter(
                        x=region_monthly['month'],
                        y=region_monthly['monthly_revenue'],
                        mode='lines+markers',
                        name=region,
                        line=dict(color=colors[i], width=3),
                        marker=dict(size=8)
                    ))
            
            fig.update_layout(
                title="📈 Évolution Mensuelle des Revenus - Top 5 Régions",
                xaxis_title="Mois",
                yaxis_title="Revenus mensuels (MAD)",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='white',
                showlegend=True,
                legend=dict(
                    bgcolor="rgba(0,0,0,0.5)",
                    bordercolor="rgba(255,255,255,0.2)",
                    borderwidth=1
                ),
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Tableau détaillé des régions
        st.markdown("#### 📊 Performances Détaillées par Région")
        
        # Formater le tableau des régions
        display_columns = ['region', 'customer_count', 'total_revenue', 'arpu', 'revenue_share', 'discount_rate']
        region_display = region_data[display_columns].copy()
        
        styled_region_df = region_display.style.format({
            'total_revenue': '{:,.0f} MAD',
            'arpu': '{:.2f} MAD',
            'revenue_share': '{:.1f}%',
            'discount_rate': '{:.1f}%',
            'customer_count': '{:,}'
        }).background_gradient(subset=['total_revenue', 'arpu'], cmap='Oranges')
        
        st.dataframe(styled_region_df, use_container_width=True, height=400)
    
    # # Prédictions et insights
    # st.markdown("### Insights et Prédictions")
    
    # col1, col2 = st.columns(2)
    
    # with col1:
    #     # Alertes automatiques
    #     if revenue_growth < -5:
    #         st.error("⚠️ Baisse significative des revenus détectée!")
    #     elif revenue_growth > 10:
    #         st.success("🎉 Croissance exceptionnelle ce mois!")
    #     else:
    #         st.info("Croissance normale des revenus")
    

        

if __name__ == "__main__":
    main()