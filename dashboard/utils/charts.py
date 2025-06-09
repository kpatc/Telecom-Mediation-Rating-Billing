import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from config import COLORS

def create_revenue_chart(df):
    """Graphique des revenus mensuels"""
    fig = px.line(df, x='month', y='total_revenue',
                  title='📈 Évolution des Revenus Mensuels',
                  labels={'total_revenue': 'Revenus (MAD)', 'month': 'Mois'},
                  color_discrete_sequence=[COLORS['primary']])
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white'
    )
    return fig

def create_service_pie_chart(df):
    """Graphique en secteurs des services"""
    fig = px.pie(df, values='total_cost', names='record_type',
                 title='💰 Répartition des Revenus par Service',
                 color_discrete_sequence=px.colors.qualitative.Set3)
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white'
    )
    return fig

def create_processing_status_chart(df):
    """Graphique des statuts de traitement"""
    fig = px.bar(df, x='date', y='count', color='status',
                 title='⚙️ Statuts de Traitement des CDR',
                 labels={'count': 'Nombre de CDR', 'date': 'Date'},
                 color_discrete_sequence=[COLORS['success'], COLORS['danger']])
    
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white'
    )
    return fig

def create_metric_card(title, value, delta=None, unit=""):
    """Créer une carte de métrique"""
    delta_color = "normal"
    if delta:
        if delta > 0:
            delta_color = "normal"
            delta = f"+{delta}"
        else:
            delta_color = "inverse"
    
    return {
        'title': title,
        'value': f"{value:,.2f}{unit}" if isinstance(value, (int, float)) else value,
        'delta': delta,
        'delta_color': delta_color
    }