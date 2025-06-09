import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
from io import BytesIO
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

from utils.database import get_all_invoices, get_invoice_details, get_billing_statistics

st.set_page_config(page_title="Billing Management", page_icon="🧾", layout="wide")

def format_currency(amount):
    """Formater un montant en MAD"""
    return f"{amount:.2f} MAD"

def format_date(date_str):
    """Formater une date"""
    if pd.isna(date_str):
        return "N/A"
    try:
        date_obj = pd.to_datetime(date_str)
        return date_obj.strftime("%B %Y")
    except:
        return str(date_str)

def create_invoice_pdf(invoice_data, usage_data, customer_id, billing_period):
    """Générer un PDF de facture"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    
    # Style personnalisé
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.blue,
        alignment=TA_CENTER,
        spaceAfter=30
    )
    
    story = []
    
    # En-tête
    title = Paragraph("FACTURE TELECOM", title_style)
    story.append(title)
    story.append(Spacer(1, 20))
    
    # Informations client
    if not invoice_data.empty:
        row = invoice_data.iloc[0]
        client_info = [
            ["Client:", row.get('customer_name', 'N/A')],
            ["ID Client:", customer_id],
            ["Plan tarifaire:", row.get('rate_plan_id', 'N/A')],
            ["Période:", format_date(billing_period)],
            ["Région:", row.get('region', 'N/A')]
        ]
        
        client_table = Table(client_info, colWidths=[2*inch, 3*inch])
        client_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(client_table)
        story.append(Spacer(1, 20))
    
    # Détail des usages
    if not usage_data.empty:
        story.append(Paragraph("Détail des Usages", styles['Heading2']))
        
        usage_table_data = [['Service', 'Quantité', 'Coût de base', 'Coût final']]
        
        for _, row in usage_data.iterrows():
            service_type = row['record_type'].upper()
            if service_type == 'VOICE':
                quantity = f"{row['total_duration_sec']//60} min"
            elif service_type == 'DATA':
                quantity = f"{row['total_data_mb']:.1f} MB"
            else:
                quantity = f"{row['usage_count']} SMS"
            
            usage_table_data.append([
                service_type,
                quantity,
                format_currency(row['total_base_cost']),
                format_currency(row['total_final_cost'])
            ])
        
        usage_table = Table(usage_table_data, colWidths=[1.5*inch, 1.5*inch, 1.5*inch, 1.5*inch])
        usage_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(usage_table)
        story.append(Spacer(1, 20))
    
    # Résumé financier
    if not invoice_data.empty:
        row = invoice_data.iloc[0]
        
        story.append(Paragraph("Résumé Financier", styles['Heading2']))
        
        financial_data = [
            ['Coût de base:', format_currency(row.get('total_base_cost', 0))],
            ['Réductions:', f"-{format_currency(row.get('total_discount', 0))}"],
            ['TVA (20%):', format_currency(row.get('tax_applied', 0))],
            ['TOTAL À PAYER:', format_currency(row.get('total_final_cost', 0))]
        ]
        
        financial_table = Table(financial_data, colWidths=[3*inch, 2*inch])
        financial_table.setStyle(TableStyle([
            ('BACKGROUND', (0, -1), (-1, -1), colors.lightblue),
            ('TEXTCOLOR', (0, -1), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -2), 'Helvetica'),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(financial_table)
    
    doc.build(story)
    buffer.seek(0)
    return buffer

# Interface principal
st.title("🧾 Gestion de la Facturation")
st.markdown("---")

# Sidebar pour les filtres
st.sidebar.header("🔍 Filtres")

# Charger les données
invoices_df = get_all_invoices()
stats_df, monthly_df = get_billing_statistics()

if not invoices_df.empty:
    # Filtres
    customers = ['Tous'] + sorted(invoices_df['customer_id'].unique().tolist())
    selected_customer = st.sidebar.selectbox("Client", customers)
    
    periods = ['Toutes'] + sorted(invoices_df['billing_period'].dt.strftime('%Y-%m').unique().tolist(), reverse=True)
    selected_period = st.sidebar.selectbox("Période", periods)
    
    # Filtrer les données
    filtered_df = invoices_df.copy()
    if selected_customer != 'Tous':
        filtered_df = filtered_df[filtered_df['customer_id'] == selected_customer]
    if selected_period != 'Toutes':
        filtered_df = filtered_df[filtered_df['billing_period'].dt.strftime('%Y-%m') == selected_period]

    # Métriques principales
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_invoices = len(filtered_df)
        st.metric("📄 Total Factures", total_invoices)
    
    with col2:
        total_revenue = filtered_df['total_final_cost'].sum()
        st.metric("💰 Chiffre d'affaires", format_currency(total_revenue))
    
    with col3:
        avg_amount = filtered_df['total_final_cost'].mean() if not filtered_df.empty else 0
        st.metric("📊 Montant moyen", format_currency(avg_amount))
    
    with col4:
        unique_customers = filtered_df['customer_id'].nunique()
        st.metric("👥 Clients uniques", unique_customers)

    # Graphiques
    if not monthly_df.empty:
        st.subheader("📈 Évolution du Chiffre d'Affaires")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Graphique en barres du CA mensuel
            fig_revenue = px.bar(
                monthly_df, 
                x='billing_period', 
                y='monthly_revenue',
                title="Chiffre d'affaires mensuel",
                labels={'monthly_revenue': 'Revenue (MAD)', 'billing_period': 'Période'}
            )
            fig_revenue.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_revenue, use_container_width=True)
        
        with col2:
            # Graphique du nombre de factures
            fig_count = px.line(
                monthly_df, 
                x='billing_period', 
                y='invoice_count',
                title="Nombre de factures par mois",
                labels={'invoice_count': 'Nombre de factures', 'billing_period': 'Période'},
                markers=True
            )
            fig_count.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_count, use_container_width=True)

    # Liste des factures
    st.subheader("📋 Liste des Factures")
    
    if not filtered_df.empty:
        # Formater les données pour l'affichage
        display_df = filtered_df.copy()
        display_df['billing_period'] = display_df['billing_period'].dt.strftime('%Y-%m')
        display_df['total_base_cost'] = display_df['total_base_cost'].apply(lambda x: f"{x:.2f} MAD")
        display_df['total_discount'] = display_df['total_discount'].apply(lambda x: f"{x:.2f} MAD")
        display_df['tax_applied'] = display_df['tax_applied'].apply(lambda x: f"{x:.2f} MAD")
        display_df['total_final_cost'] = display_df['total_final_cost'].apply(lambda x: f"{x:.2f} MAD")
        
        # Renommer les colonnes
        display_df = display_df.rename(columns={
            'invoice_id': 'ID Facture',
            'customer_id': 'ID Client',
            'customer_name': 'Nom Client',
            'billing_period': 'Période',
            'total_base_cost': 'Coût de base',
            'total_discount': 'Réductions',
            'tax_applied': 'TVA',
            'total_final_cost': 'Montant total',
            'rate_plan_id': 'Plan tarifaire'
        })
        
        # Afficher le tableau avec sélection
        selected_indices = st.dataframe(
            display_df.drop('ID Facture', axis=1),
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row"
        )
        
        # Détails de la facture sélectionnée
        if selected_indices.selection.rows:
            selected_idx = selected_indices.selection.rows[0]
            selected_invoice = filtered_df.iloc[selected_idx]
            
            st.markdown("---")
            st.subheader(f"📄 Détails de la facture - {selected_invoice['customer_name']}")
            
            # Récupérer les détails
            invoice_details, usage_details = get_invoice_details(
                selected_invoice['customer_id'],
                selected_invoice['billing_period'].strftime('%Y-%m-%d')
            )
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                if not usage_details.empty:
                    st.write("**Détail des usages:**")
                    
                    # Formatage des usages
                    usage_display = usage_details.copy()
                    usage_display['Service'] = usage_display['record_type'].str.upper()
                    usage_display['Quantité'] = usage_display.apply(lambda row: 
                        f"{row['total_duration_sec']//60} minutes" if row['record_type'] == 'voice'
                        else f"{row['total_data_mb']:.1f} MB" if row['record_type'] == 'data'
                        else f"{row['usage_count']} SMS", axis=1
                    )
                    usage_display['Coût de base'] = usage_display['total_base_cost'].apply(format_currency)
                    usage_display['Coût final'] = usage_display['total_final_cost'].apply(format_currency)
                    
                    st.dataframe(
                        usage_display[['Service', 'Quantité', 'Coût de base', 'Coût final']],
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.info("Aucun détail d'usage disponible pour cette facture.")
            
            with col2:
                st.write("**Résumé financier:**")
                
                summary_data = {
                    "Coût de base": format_currency(selected_invoice['total_base_cost']),
                    "Réductions": f"-{format_currency(selected_invoice['total_discount'])}",
                    "TVA (20%)": format_currency(selected_invoice['tax_applied']),
                    "**Total à payer**": f"**{format_currency(selected_invoice['total_final_cost'])}**"
                }
                
                for label, value in summary_data.items():
                    st.write(f"{label}: {value}")
                
                # Bouton de téléchargement PDF
                st.markdown("---")
                if st.button("📥 Télécharger en PDF", type="primary", use_container_width=True):
                    pdf_buffer = create_invoice_pdf(
                        invoice_details, 
                        usage_details,
                        selected_invoice['customer_id'],
                        selected_invoice['billing_period'].strftime('%Y-%m-%d')
                    )
                    
                    st.download_button(
                        label="💾 Télécharger la facture",
                        data=pdf_buffer.getvalue(),
                        file_name=f"facture_{selected_invoice['customer_id']}_{selected_invoice['billing_period'].strftime('%Y_%m')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
        
        # Possibilité de télécharger la liste complète
        st.markdown("---")
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col2:
            if st.button("📊 Exporter la liste (CSV)", use_container_width=True):
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="💾 Télécharger CSV",
                    data=csv,
                    file_name=f"factures_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
    
    else:
        st.warning("Aucune facture trouvée avec les filtres sélectionnés.")

else:
    st.error("❌ Aucune donnée de facturation disponible.")
    st.info("Assurez-vous que le moteur de facturation a été exécuté et que des factures ont été générées.")

# Section d'information
with st.expander("ℹ️ Informations sur la facturation"):
    st.markdown("""
    ### 📋 Processus de facturation
    
    1. **Génération mensuelle** : Les factures sont générées automatiquement chaque mois
    2. **Calcul des coûts** : Basé sur les usages notés (rated_cdr)
    3. **Réductions appliquées** :
       - Unités gratuites incluses dans les plans
       - Réduction étudiante (10%)
       - Réduction fidélité (5% après 12 mois)
    4. **Taxes** : TVA de 20% + frais réglementaires (1 MAD)
    
    ### 📊 Métriques disponibles
    - Chiffre d'affaires total et mensuel
    - Nombre de factures par période
    - Montant moyen par facture
    - Détail des usages par service (voice, SMS, data)
    """)