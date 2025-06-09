import os
from dotenv import load_dotenv

load_dotenv()

# Configuration base de données
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Configuration du dashboard
DASHBOARD_CONFIG = {
    'title': '📊 Telecom Analytics Dashboard',
    'layout': 'wide',
    'theme': 'dark',
    'refresh_interval': 30  # secondes
}

# Couleurs du thème
COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e',
    'success': '#2ca02c',
    'danger': '#d62728',
    'warning': '#ff7f0e',
    'info': '#17a2b8'
}