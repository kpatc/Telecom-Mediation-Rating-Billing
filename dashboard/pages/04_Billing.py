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

from utils.database import get_all_invoices, get_invoice_details, get_billing_statistics, map_city_to_region

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
    """Générer un PDF de facture avec un design moderne et stylé"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=0.5*inch, bottomMargin=0.5*inch)
    styles = getSampleStyleSheet()
    
    # Styles personnalisés modernes
    company_style = ParagraphStyle(
        'CompanyStyle',
        parent=styles['Normal'],
        fontSize=16,
        textColor=colors.HexColor('#1e40af'),
        alignment=TA_CENTER,
        fontName='Helvetica-Bold',
        spaceAfter=10
    )
    
    title_style = ParagraphStyle(
        'InvoiceTitle',
        parent=styles['Heading1'],
        fontSize=28,
        textColor=colors.HexColor('#059669'),
        alignment=TA_CENTER,
        fontName='Helvetica-Bold',
        spaceAfter=30
    )
    
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Normal'],
        fontSize=14,
        textColor=colors.HexColor('#374151'),
        fontName='Helvetica-Bold',
        spaceAfter=15
    )
    
    info_style = ParagraphStyle(
        'InfoStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.HexColor('#6b7280'),
        alignment=TA_CENTER,
        spaceAfter=20
    )
    
    story = []
    
    # En-tête moderne avec logo et couleurs
    header_table = Table([
        [Paragraph(" TELECOM MAROC", company_style), 
         Paragraph(f"Facture N° {invoice_data.iloc[0].get('invoice_id', 'N/A') if not invoice_data.empty else 'N/A'}", subtitle_style)]
    ], colWidths=[3*inch, 3*inch])
    
    header_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, 0), colors.HexColor('#f0f9ff')),
        ('BACKGROUND', (1, 0), (1, 0), colors.HexColor('#f0fdf4')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#d1d5db')),
        ('ROUNDEDCORNERS', [5, 5, 5, 5]),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 15),
        ('TOPPADDING', (0, 0), (-1, -1), 15)
    ]))
    
    story.append(header_table)
    story.append(Spacer(1, 20))
    
    # Informations client avec design moderne
    if not invoice_data.empty:
        row = invoice_data.iloc[0]
        
        # Section informations client
        story.append(Paragraph("INFORMATIONS CLIENT", subtitle_style))
        
        client_info = [
            ["👤 Client:", row.get('customer_name', 'N/A')],
            ["🆔 ID Client:", customer_id],
            ["📋 Plan tarifaire:", row.get('rate_plan_id', 'N/A')],
            ["📅 Période:", format_date(billing_period)],
            ["📍 Région:", row.get('region_mapped', row.get('region', 'N/A'))],
            ["🎓 Statut:", "Étudiant" if row.get('is_student', False) else "Standard"]
        ]
        
        client_table = Table(client_info, colWidths=[2*inch, 3.5*inch])
        client_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f8fafc')),
            ('BACKGROUND', (1, 0), (1, -1), colors.white),
            ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#1f2937')),
            ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#374151')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 11),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#e5e7eb')),
            ('ROUNDEDCORNERS', [3, 3, 3, 3])
        ]))
        
        story.append(client_table)
        story.append(Spacer(1, 25))
    
    # Détail des usages avec design moderne
    if not usage_data.empty:
        story.append(Paragraph("DÉTAIL DES SERVICES UTILISÉS", subtitle_style))
        
        usage_table_data = [['🔧 Service', '📊 Quantité', '💰 Coût de base', '✨ Coût final']]
        
        for _, row in usage_data.iterrows():
            service_type = row['record_type'].upper()
            
            # Icônes et formatage par service
            if service_type == 'VOICE':
                service_icon = "📞 APPELS"
                quantity = f"{int(row['total_duration_sec'])//60} minutes"
            elif service_type == 'DATA':
                service_icon = "📱 DONNÉES"
                quantity = f"{row['total_data_mb']:.1f} MB"
            else:
                service_icon = "💬 SMS"
                quantity = f"{int(row['usage_count'])} messages"
            
            usage_table_data.append([
                service_icon,
                quantity,
                format_currency(row['total_base_cost']),
                format_currency(row['total_final_cost'])
            ])
        
        usage_table = Table(usage_table_data, colWidths=[1.8*inch, 1.5*inch, 1.5*inch, 1.5*inch])
        usage_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f2937')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
            ('TOPPADDING', (0, 0), (-1, -1), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f9fafb')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f9fafb')]),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#d1d5db')),
            ('ROUNDEDCORNERS', [5, 5, 5, 5])
        ]))
        
        story.append(usage_table)
        story.append(Spacer(1, 25))
    
    # Résumé financier moderne avec gradient visuel
    if not invoice_data.empty:
        row = invoice_data.iloc[0]
        
        story.append(Paragraph("RÉSUMÉ FINANCIER", subtitle_style))
        
        financial_data = [
            ['💵 Coût de base:', format_currency(row.get('total_base_cost', 0))],
            ['🎯 Réductions appliquées:', f"-{format_currency(row.get('total_discount', 0))}"],
            ['📋 TVA (20%):', format_currency(row.get('tax_applied', 0))],
            ['', ''],  # Ligne de séparation
            ['💳 TOTAL À PAYER:', format_currency(row.get('total_final_cost', 0))]
        ]
        
        financial_table = Table(financial_data, colWidths=[3.5*inch, 2*inch])
        financial_table.setStyle(TableStyle([
            # Styles pour les lignes normales
            ('BACKGROUND', (0, 0), (0, 2), colors.HexColor('#f8fafc')),
            ('BACKGROUND', (1, 0), (1, 2), colors.white),
            # Ligne de séparation
            ('BACKGROUND', (0, 3), (-1, 3), colors.white),
            ('LINEBELOW', (0, 2), (-1, 2), 2, colors.HexColor('#d1d5db')),
            # Ligne totale avec style spécial
            ('BACKGROUND', (0, 4), (-1, 4), colors.HexColor('#059669')),
            ('TEXTCOLOR', (0, 4), (-1, 4), colors.white),
            ('FONTNAME', (0, 4), (-1, 4), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 4), (-1, 4), 14),
            # Styles généraux
            ('TEXTCOLOR', (0, 0), (0, 2), colors.HexColor('#1f2937')),
            ('TEXTCOLOR', (1, 0), (1, 2), colors.HexColor('#374151')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, 2), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, 2), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, 2), 12),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
            ('TOPPADDING', (0, 0), (-1, -1), 12),
            ('GRID', (0, 0), (-1, 2), 1, colors.HexColor('#e5e7eb')),
            ('GRID', (0, 4), (-1, 4), 2, colors.HexColor('#047857')),
            ('ROUNDEDCORNERS', [5, 5, 5, 5])
        ]))
        
        story.append(financial_table)
        story.append(Spacer(1, 30))
    
    # Pied de page moderne
    footer_text = f"""
    <para alignment="center" fontSize="9" textColor="#6b7280">
    🏢 Telecom Maroc - Service Client: 📞 +212 5XX-XXXXX - 📧 contact@telecom.ma<br/>
    📍 Adresse: Avenue Mohammed V, Casablanca, Maroc<br/>
    🌐 www.telecom.ma | Facture générée le {datetime.now().strftime('%d/%m/%Y à %H:%M')}
    </para>
    """
    
    story.append(Paragraph(footer_text, info_style))
    
    # Note de service
    note_text = """
    <para alignment="center" fontSize="8" textColor="#9ca3af">
    💡 Cette facture est générée automatiquement par notre système de facturation.<br/>
    En cas de questions, contactez notre service client.
    </para>
    """
    story.append(Spacer(1, 10))
    story.append(Paragraph(note_text, info_style))
    
    doc.build(story)
    buffer.seek(0)
    return buffer

# Interface principal
st.title("🧾 Gestion de la Facturation")
st.markdown("---")

# Sidebar pour les filtres
st.sidebar.header("🔍 Filtres")

# Charger les données
with st.spinner("Chargement des données de facturation..."):
    invoices_df = get_all_invoices()
    stats_df, monthly_df = get_billing_statistics()

# Ajouter les styles CSS pour améliorer l'apparence
st.markdown("""
<style>
.stButton > button[kind="secondary"] {
    background-color: #059669 !important;
    color: white !important;
    border: none !important;
}
.stButton > button[kind="secondary"]:hover {
    background-color: #047857 !important;
}
</style>
""", unsafe_allow_html=True)
        
if not invoices_df.empty and 'region' in invoices_df.columns:
    invoices_df['region_mapped'] = invoices_df['region'].apply(map_city_to_region)

if not invoices_df.empty:
    # Filtres sans le filtre client
    periods = ['Toutes'] + sorted(invoices_df['billing_period'].dt.strftime('%Y-%m').unique().tolist(), reverse=True)
    selected_period = st.sidebar.selectbox("📅 Période", periods)
    
    # Filtre par région
    if 'region_mapped' in invoices_df.columns:
        regions = ['Toutes'] + sorted([str(x) for x in invoices_df['region_mapped'].unique() if pd.notna(x) and x != 'Autres'])
        selected_region = st.sidebar.selectbox("📍 Région", regions)
    else:
        selected_region = 'Toutes'
    
    # Filtrer les données
    filtered_df = invoices_df.copy()
    
    if selected_period != 'Toutes':
        filtered_df = filtered_df[filtered_df['billing_period'].dt.strftime('%Y-%m') == selected_period]
    
    if selected_region != 'Toutes' and 'region_mapped' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['region_mapped'] == selected_region]

    # Métriques principales avec design amélioré
    st.markdown("### 📊 Métriques de Facturation")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_invoices = len(filtered_df)
        st.metric(
            "📄 Total Factures", 
            total_invoices,
            delta=f"{total_invoices - len(invoices_df)} vs total" if len(invoices_df) > 0 else None
        )
    
    with col2:
        total_revenue = filtered_df['total_final_cost'].sum() if not filtered_df.empty else 0
        st.metric(
            "💰 Chiffre d'affaires", 
            format_currency(total_revenue),
            delta=f"{(total_revenue/invoices_df['total_final_cost'].sum()*100):.1f}% du total" if not invoices_df.empty and invoices_df['total_final_cost'].sum() > 0 else None
        )
    
    with col3:
        avg_amount = filtered_df['total_final_cost'].mean() if not filtered_df.empty else 0
        st.metric(
            "📊 Montant moyen", 
            format_currency(avg_amount),
            delta=f"vs {format_currency(invoices_df['total_final_cost'].mean())}" if not invoices_df.empty else None
        )
    
    with col4:
        unique_customers = filtered_df['customer_id'].nunique() if not filtered_df.empty else 0
        st.metric(
            "👥 Clients uniques", 
            unique_customers,
            delta=f"{(unique_customers/invoices_df['customer_id'].nunique()*100):.1f}% du total" if not invoices_df.empty else None
        )

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

    # Liste des factures avec pagination
    st.subheader("📋 Liste des Factures")
    
    if not filtered_df.empty:
        # Pagination
        items_per_page = 5
        total_items = len(filtered_df)
        total_pages = (total_items - 1) // items_per_page + 1
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            page_number = st.selectbox(
                f"Page (Total: {total_pages} pages, {total_items} factures)",
                range(1, total_pages + 1),
                key="page_selector"
            )
        
        # Calculer les indices pour la pagination
        start_idx = (page_number - 1) * items_per_page
        end_idx = min(start_idx + items_per_page, total_items)
        
        # Afficher les factures avec boutons de téléchargement
        for idx, (_, row) in enumerate(filtered_df.iloc[start_idx:end_idx].iterrows()):
            with st.container():
                # Créer une ligne avec les informations de la facture et le bouton de téléchargement
                col1, col2, col3, col4, col5, col6 = st.columns([1, 1.5, 1, 1, 1, 1.2])
                
                with col1:
                    st.write(f"**{row['customer_id']}**")
                    st.caption("ID Client")
                
                with col2:
                    st.write(f"**{row.get('customer_name', 'N/A')}**")
                    st.caption("Nom Client")
                
                with col3:
                    st.write(f"**{row['billing_period'].strftime('%Y-%m')}**")
                    st.caption("Période")
                
                with col4:
                    st.write(f"**{row.get('region_mapped', row.get('region', 'N/A'))}**")
                    st.caption("Région")
                
                with col5:
                    st.write(f"**{format_currency(row['total_final_cost'])}**")
                    st.caption("Montant Total")
                
                with col6:
                    # Bouton de téléchargement PDF pour cette facture avec couleur verte
                    if st.button(
                        "📄 PDF", 
                        key=f"download_{row['customer_id']}_{row['billing_period'].strftime('%Y_%m')}_{page_number}", 
                        type="secondary",
                        use_container_width=True
                    ):
                        try:
                            with st.spinner("🔄 Génération de la facture PDF..."):
                                # Récupérer les détails de la facture
                                invoice_details, usage_details = get_invoice_details(
                                    row['customer_id'],
                                    row['billing_period'].strftime('%Y-%m-%d')
                                )
                                
                                # Créer le PDF
                                pdf_buffer = create_invoice_pdf(
                                    invoice_details if not invoice_details.empty else pd.DataFrame([row]),
                                    usage_details,
                                    row['customer_id'],
                                    row['billing_period'].strftime('%Y-%m-%d')
                                )
                            
                            st.success("✅ Facture PDF générée!")
                            
                            # Bouton de téléchargement
                            st.download_button(
                                label=f"💾 Télécharger facture {row['customer_id']}",
                                data=pdf_buffer.getvalue(),
                                file_name=f"facture_{row['customer_id']}_{row['billing_period'].strftime('%Y_%m')}.pdf",
                                mime="application/pdf",
                                key=f"dl_{row['customer_id']}_{row['billing_period'].strftime('%Y_%m')}_{page_number}"
                            )
                            
                        except Exception as e:
                            st.error(f"❌ Erreur lors de la génération du PDF: {str(e)}")
                
                # Ajouter une ligne de séparation sauf pour le dernier élément
                if idx < len(filtered_df.iloc[start_idx:end_idx]) - 1:
                    st.divider()
        
        # Informations de pagination
        st.info(f"📄 Page {page_number} sur {total_pages} | Affichage des factures {start_idx + 1} à {end_idx} sur {total_items}")
        
        # Section d'export avec design amélioré
        st.markdown("---")
        st.markdown("### 📊 Exporter les données")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            # Créer un expander pour les options d'export
            with st.expander("🔽 Options d'export", expanded=False):
                export_format = st.selectbox(
                    "Format d'export:",
                    ["CSV", "Excel", "JSON"],
                    index=0
                )
                
                include_details = st.checkbox("Inclure les détails étendus", value=True)
                
                if st.button("📊 Exporter la liste complète", type="primary", use_container_width=True):
                    try:
                        with st.spinner(f"🔄 Génération du fichier {export_format}..."):
                            # Préparer les données pour l'export
                            export_df = filtered_df.copy()
                            
                            if include_details:
                                # Ajouter plus de détails
                                export_df['region_mapped'] = export_df['region'].apply(
                                    lambda x: map_city_to_region(x) if 'map_city_to_region' in globals() else x
                                )
                            
                            # Renommer les colonnes pour l'export
                            export_df = export_df.rename(columns={
                                'invoice_id': 'ID_Facture',
                                'customer_id': 'ID_Client',
                                'customer_name': 'Nom_Client',
                                'billing_period': 'Periode_Facturation',
                                'total_base_cost': 'Cout_Base',
                                'total_discount': 'Reductions',
                                'tax_applied': 'TVA',
                                'total_final_cost': 'Montant_Total',
                                'rate_plan_id': 'Plan_Tarifaire',
                                'region_mapped': 'Region'
                            })
                            
                            if export_format == "CSV":
                                csv = export_df.to_csv(index=False)
                                st.download_button(
                                    label="💾 Télécharger CSV",
                                    data=csv,
                                    file_name=f"factures_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                                    mime="text/csv",
                                    use_container_width=True
                                )
                            elif export_format == "Excel":
                                # Pour Excel, on utiliserait to_excel() mais nous gardons CSV pour la simplicité
                                csv = export_df.to_csv(index=False)
                                st.download_button(
                                    label="💾 Télécharger Excel (CSV)",
                                    data=csv,
                                    file_name=f"factures_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                                    mime="text/csv",
                                    use_container_width=True
                                )
                            else:  # JSON
                                json_data = export_df.to_json(orient='records', indent=2)
                                st.download_button(
                                    label="💾 Télécharger JSON",
                                    data=json_data,
                                    file_name=f"factures_{datetime.now().strftime('%Y%m%d_%H%M')}.json",
                                    mime="application/json",
                                    use_container_width=True
                                )
                        
                        st.success(f"✅ Fichier {export_format} généré avec succès!")
                        
                    except Exception as e:
                        st.error(f"❌ Erreur lors de l'export: {str(e)}")
                        st.info("Veuillez réessayer ou contacter l'administrateur.")
    
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