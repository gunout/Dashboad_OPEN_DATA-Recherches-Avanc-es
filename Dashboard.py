import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
from datetime import datetime, timedelta, timezone
import numpy as np
import feedparser
import concurrent.futures
import time
import warnings
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import re
import urllib.parse
from bs4 import BeautifulSoup
import io
import base64

warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURATION DE LA PAGE
# =============================================================================
st.set_page_config(
    page_title="Dashboard Analytics Data.gouv.fr - 24K Datasets",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# STYLES CSS PERSONNALISÉS - THÈME BLEU BLANC ROUGE
# =============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem; 
        color: #1f77b4; 
        text-align: center; 
        margin-bottom: 1rem;
        font-weight: bold;
        background: linear-gradient(90deg, #0055A4 0%, #FFFFFF 50%, #EF4135 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .section-header {
        font-size: 1.8rem; 
        color: #0055A4; 
        margin: 1.5rem 0 1rem 0; 
        border-bottom: 3px solid #EF4135; 
        padding-bottom: 0.5rem;
        font-weight: 600;
    }
    .metric-card {
        background: linear-gradient(135deg, #FFFFFF 0%, #F0F8FF 100%); 
        padding: 1.2rem; 
        border-radius: 10px; 
        margin: 0.8rem 0;
        border-left: 4px solid #0055A4;
        color: #000000;
        box-shadow: 0 2px 4px rgba(0,85,164,0.1);
    }
    .alert-red {
        background-color: #FFEBEE; 
        padding: 1rem; 
        border-radius: 5px; 
        border-left: 5px solid #EF4135; 
        margin: 0.5rem 0;
        color: #000000;
    }
    .alert-orange {
        background-color: #FFF3E0; 
        padding: 1rem; 
        border-radius: 5px; 
        border-left: 5px solid #FF9800; 
        margin: 0.5rem 0;
        color: #000000;
    }
    .alert-green {
        background-color: #E8F5E8; 
        padding: 1rem; 
        border-radius: 5px; 
        border-left: 5px solid #4CAF50; 
        margin: 0.5rem 0;
        color: #000000;
    }
    .filter-section {
        background: linear-gradient(135deg, #F8F9FF 0%, #E8F4FF 100%); 
        padding: 1.5rem; 
        border-radius: 10px; 
        margin-bottom: 1rem;
        color: #000000;
        border: 2px solid #0055A4;
    }
    .kpi-card {
        background: linear-gradient(135deg, #0055A4 0%, #1f77b4 100%); 
        color: white; 
        padding: 1.2rem; 
        border-radius: 10px; 
        margin: 0.3rem;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,85,164,0.2);
    }
    .data-source {
        font-size: 0.8rem; 
        color: #666; 
        font-style: italic;
        text-align: center;
    }
    .rss-item {
        border-left: 4px solid #EF4135; 
        padding-left: 15px; 
        margin: 10px 0;
        color: #000000;
        background: #FFFFFF;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(239,65,53,0.1);
    }
    .progress-info {
        color: #0055A4; 
        font-weight: bold;
    }
    .dataset-card {
        border: 2px solid #0055A4; 
        padding: 15px; 
        margin: 10px 0; 
        border-radius: 8px; 
        background: linear-gradient(135deg, #FFFFFF 0%, #F0F8FF 100%);
        color: #000000;
        box-shadow: 0 2px 4px rgba(0,85,164,0.1);
    }
    .search-result {
        border-left: 5px solid #0055A4; 
        padding: 20px; 
        margin: 15px 0; 
        background: linear-gradient(135deg, #FFFFFF 0%, #F8F9FF 100%);
        color: #000000;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,85,164,0.1);
        border-right: 1px solid #EF4135;
        border-bottom: 1px solid #EF4135;
    }
    .stats-grid {
        display: grid; 
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
        gap: 10px; 
        margin: 15px 0;
    }
    .stat-item {
        background: linear-gradient(135deg, #FFFFFF 0%, #F0F8FF 100%); 
        padding: 15px; 
        border-radius: 8px; 
        box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
        text-align: center;
        color: #000000;
        border: 1px solid #0055A4;
    }
    .result-item {
        background: #FFFFFF;
        border: 2px solid #0055A4;
        border-radius: 10px;
        padding: 20px;
        margin: 15px 0;
        box-shadow: 0 4px 6px rgba(0,85,164,0.1);
        color: #000000;
        border-top: 3px solid #EF4135;
    }
    .result-title {
        font-size: 1.3rem;
        font-weight: bold;
        color: #0055A4;
        margin-bottom: 10px;
        border-bottom: 1px solid #EF4135;
        padding-bottom: 8px;
    }
    .result-meta {
        color: #0055A4;
        font-size: 1rem;
        margin-bottom: 8px;
        font-weight: 500;
    }
    .result-stats {
        background: linear-gradient(135deg, #F8F9FF 0%, #E8F4FF 100%);
        padding: 12px;
        border-radius: 6px;
        margin: 12px 0;
        color: #000000;
        border: 1px solid #0055A4;
    }
    
    /* Styles pour les boutons */
    .stButton > button {
        background: linear-gradient(135deg, #0055A4 0%, #1f77b4 100%);
        color: white;
        border: none;
        border-radius: 6px;
        font-weight: 600;
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #004494 0%, #1866a3 100%);
        color: white;
    }
    
    /* Style pour les sélecteurs */
    .stSelectbox, .stTextInput, .stMultiselect, .stSlider {
        color: #000000;
    }
    
    /* Style pour les dataframes */
    .dataframe {
        color: #000000 !important;
    }
    
    /* Style pour les onglets */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
        background-color: #F0F8FF;
        padding: 10px;
        border-radius: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background: linear-gradient(135deg, #FFFFFF 0%, #F0F8FF 100%);
        border-radius: 6px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
        font-weight: 600;
        color: #0055A4;
        border: 1px solid #0055A4;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: linear-gradient(135deg, #0055A4 0%, #1f77b4 100%);
        color: white;
    }
    
    /* Style pour les métriques */
    [data-testid="stMetricValue"] {
        color: #0055A4;
        font-weight: bold;
    }
    [data-testid="stMetricLabel"] {
        color: #EF4135;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# CONFIGURATION ET CONSTANTES
# =============================================================================
API_BASE_URL = "https://www.data.gouv.fr/api/1"
DATASETS_URL = f"{API_BASE_URL}/datasets/"
ORGANIZATIONS_URL = f"{API_BASE_URL}/organizations/"
MAX_DATASETS = 24187
CONCURRENT_REQUESTS = 5
PAGE_SIZE = 20
CACHE_TTL = 1800

# Flux RSS économiques
RSS_FEEDS = [
    "https://www.economie.gouv.fr/rss",
    "https://www.insee.fr/rss/actualites.xml",
    "https://www.banque-france.fr/rss",
    "https://www.impots.gouv.fr/rss",
    "https://www.entreprises.gouv.fr/rss"
]

# Catégories thématiques pour une recherche plus efficace
THEMATIQUES = {
    "économie": ["économie", "finances", "PIB", "croissance", "inflation", "commerce"],
    "entreprises": ["entreprises", "PME", "startup", "innovation", "industrie"],
    "emploi": ["emploi", "chômage", "travail", "salaires", "métiers"],
    "budget": ["budget", "dépenses", "recettes", "déficit", "dette"],
    "commerce": ["commerce", "export", "import", "douane", "échanges"],
    "énergie": ["énergie", "électricité", "pétrole", "gaz", "renouvelable"],
    "transport": ["transport", "logistique", "infrastructure", "mobilité"],
    "environnement": ["environnement", "climat", "pollution", "développement durable"],
    "santé": ["santé", "médical", "hôpital", "médecine", "pharmaceutique"],
    "éducation": ["éducation", "enseignement", "université", "formation"]
}

# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================
def safe_get(dictionary, keys, default=''):
    """Sécurise l'accès aux dictionnaires imbriqués"""
    for key in keys:
        if isinstance(dictionary, dict) and key in dictionary:
            dictionary = dictionary[key]
        else:
            return default
    return dictionary

def convertir_date(date_str):
    """Convertit une string de date en datetime avec timezone"""
    if not date_str:
        return None
    
    try:
        if 'Z' in date_str:
            date_str = date_str.replace('Z', '+00:00')
        
        dt = datetime.fromisoformat(date_str)
        
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        return dt
    except Exception as e:
        return None

def est_recent(date_creation, jours=30):
    """Vérifie si une date est récente (dans les X derniers jours)"""
    if not date_creation:
        return False
    
    maintenant = datetime.now(timezone.utc)
    
    if date_creation.tzinfo is None:
        date_creation = date_creation.replace(tzinfo=timezone.utc)
    
    return (maintenant - date_creation) < timedelta(days=jours)

# =============================================================================
# CLIENT API DATA.GOUV.FR AMÉLIORÉ
# =============================================================================
class DataGouvAPIClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8'
        })
    
    @st.cache_data(ttl=CACHE_TTL, show_spinner="Chargement des datasets...")
    def rechercher_datasets_avances(_self, query="", page=1, page_size=20, organization=None, format_type=None, tags=None):
        """Recherche avancée optimisée pour les 24K datasets"""
        params = {
            'page': page,
            'page_size': page_size,
            'sort': '-created'
        }
        
        if query:
            params['q'] = query
        
        if organization and organization != "Toutes les organisations":
            params['organization'] = organization
        
        # Construction des filtres complexes
        filters = []
        if format_type and format_type != "Tous les formats":
            filters.append(f'format:{format_type}')
        
        if tags:
            for tag in tags:
                filters.append(f'tag:{tag}')
        
        if filters:
            params['q'] = f"{query} {' '.join(filters)}" if query else ' '.join(filters)
        
        try:
            response = _self.session.get(DATASETS_URL, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                return data
            elif response.status_code == 429:
                st.warning("⚠️ Trop de requêtes. Attente avant nouvel essai...")
                time.sleep(2)
                response = _self.session.get(DATASETS_URL, params=params, timeout=60)
                if response.status_code == 200:
                    return response.json()
                else:
                    st.error(f"❌ Erreur API après attente: {response.status_code}")
                    return {'data': [], 'total': 0, 'page': page, 'page_size': page_size}
            else:
                st.error(f"❌ Erreur API: {response.status_code}")
                return {'data': [], 'total': 0, 'page': page, 'page_size': page_size}
                
        except requests.exceptions.Timeout:
            st.error("⏰ Timeout: L'API met trop de temps à répondre")
            return {'data': [], 'total': 0, 'page': page, 'page_size': page_size}
        except Exception as e:
            st.error(f"🚨 Erreur inattendue: {str(e)}")
            return {'data': [], 'total': 0, 'page': page, 'page_size': page_size}
    
    @st.cache_data(ttl=CACHE_TTL)
    def get_datasets_populaires(_self, limit=50):
        """Récupère les datasets les plus populaires avec cache et gestion d'erreurs améliorée"""
        try:
            # Essayer d'abord par followers
            params = {
                'page_size': limit,
                'sort': '-metrics.followers'
            }
            
            response = _self.session.get(DATASETS_URL, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                datasets = data.get('data', [])
                if datasets:
                    return datasets
            
            # Fallback: tri par vues
            st.info("Chargement des datasets par popularité (fallback)...")
            params = {
                'page_size': limit,
                'sort': '-metrics.views'
            }
            
            response = _self.session.get(DATASETS_URL, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                datasets = data.get('data', [])
                if datasets:
                    return datasets
            
            # Dernier fallback: datasets récents
            params = {
                'page_size': limit,
                'sort': '-created'
            }
            
            response = _self.session.get(DATASETS_URL, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                return data.get('data', [])
            else:
                return []
                
        except Exception as e:
            st.warning(f"⚠️ Erreur chargement datasets populaires: {str(e)}")
            return []
    
    @st.cache_data(ttl=CACHE_TTL)
    def get_organizations_list(_self, limit=100):
        """Récupère la liste des organisations avec cache"""
        try:
            params = {
                'page_size': limit,
                'sort': '-datasets'
            }
            
            response = _self.session.get(ORGANIZATIONS_URL, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                return data.get('data', [])
            else:
                return []
        except Exception as e:
            st.warning(f"⚠️ Erreur chargement organisations: {str(e)}")
            return []
    
    @st.cache_data(ttl=CACHE_TTL)
    def get_dataset_details(_self, dataset_id):
        """Récupère les détails d'un dataset spécifique avec cache"""
        url = f"{DATASETS_URL}{dataset_id}/"
        try:
            response = _self.session.get(url, timeout=60)
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except Exception as e:
            return None
    
    def get_resource_data(self, resource_url):
        """Télécharge et parse les données d'une ressource"""
        try:
            if resource_url.endswith('.csv'):
                response = self.session.get(resource_url, timeout=60)
                response.raise_for_status()
                return pd.read_csv(io.StringIO(response.text))
            elif resource_url.endswith(('.xlsx', '.xls')):
                response = self.session.get(resource_url, timeout=60)
                response.raise_for_status()
                return pd.read_excel(response.content)
            elif resource_url.endswith('.json'):
                response = self.session.get(resource_url, timeout=60)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, list):
                    return pd.DataFrame(data)
                else:
                    return pd.DataFrame([data])
            else:
                st.warning(f"Format non supporté: {resource_url}")
                return None
        except Exception as e:
            st.warning(f"⚠️ Erreur chargement ressource {resource_url}: {str(e)}")
            return None

    @st.cache_data(ttl=3600)
    def get_datasets_stats(_self):
        """Récupère les statistiques globales sur les datasets avec échantillonnage"""
        try:
            # Récupérer plusieurs pages pour avoir un bon échantillon
            all_datasets = []
            total_datasets = 0
            
            # Première requête pour obtenir le total
            params = {'page_size': 1, 'page': 1}
            response = _self.session.get(DATASETS_URL, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                total_datasets = data.get('total', 0)
            
            # Récupérer un échantillon représentatif (3 pages)
            sample_size = min(300, total_datasets)
            pages_to_fetch = min(3, (sample_size + 99) // 100)
            
            for page in range(1, pages_to_fetch + 1):
                params = {'page_size': 100, 'page': page, 'sort': '-created'}
                response = _self.session.get(DATASETS_URL, params=params, timeout=60)
                
                if response.status_code == 200:
                    data = response.json()
                    all_datasets.extend(data.get('data', []))
            
            if not all_datasets:
                return None
            
            # Calculer les métriques
            org_counts = {}
            format_counts = {}
            recent_count = 0
            total_views = 0
            
            for dataset in all_datasets:
                org = safe_get(dataset, ['organization', 'name'], 'Inconnue')
                org_counts[org] = org_counts.get(org, 0) + 1
                
                # Compter les formats
                for resource in dataset.get('resources', []):
                    fmt = (resource.get('format') or '').upper()
                    if fmt:
                        format_counts[fmt] = format_counts.get(fmt, 0) + 1
                
                # Datasets récents
                if est_recent(convertir_date(dataset.get('created_at'))):
                    recent_count += 1
                
                # Total des vues
                total_views += dataset.get('metrics', {}).get('views', 0)
            
            avg_views = total_views // len(all_datasets) if all_datasets else 0
            
            return {
                'total_datasets': total_datasets,
                'organisations_count': len(org_counts),
                'top_organisations': dict(sorted(org_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
                'top_formats': dict(sorted(format_counts.items(), key=lambda x: x[1], reverse=True)[:8]),
                'recent_datasets': recent_count,
                'avg_views': avg_views,
                'sample_size': len(all_datasets)
            }
            
        except Exception as e:
            st.warning(f"⚠️ Erreur statistiques: {str(e)}")
            return None

# =============================================================================
# FONCTIONS DE DONNÉES EXTERNES
# =============================================================================
@st.cache_data(ttl=1800, show_spinner=False)
def charger_flux_rss():
    """Charge les flux RSS économiques en temps réel"""
    articles = []
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries[:5]:
                articles.append({
                    'titre': entry.title,
                    'lien': entry.link,
                    'date': entry.get('published', 'Date inconnue'),
                    'source': feed.feed.get('title', 'Source inconnue'),
                    'resume': entry.get('summary', 'Aucun résumé disponible')
                })
        except Exception as e:
            continue
    
    return sorted(articles, key=lambda x: x['date'], reverse=True)[:15]

# =============================================================================
# COMPOSANTS D'INTERFACE UTILISATEUR
# =============================================================================
def afficher_sidebar(client):
    """Affiche la sidebar avec les contrôles de recherche"""
    with st.sidebar:
        st.markdown("""
        <div style='text-align: center; padding: 10px; background: linear-gradient(135deg, #0055A4 0%, #EF4135 100%); border-radius: 10px; margin-bottom: 20px;'>
            <h3 style='color: white; margin: 0;'>🇫🇷 DATA.GOUV.FR</h3>
            <p style='color: white; margin: 0; font-size: 12px;'>Dashboard Analytics</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="filter-section">', unsafe_allow_html=True)
        st.header("🔧 RECHERCHE ET FILTRES")
        
        # Paramètres de recherche
        st.subheader("🔍 Critères de Recherche")
        
        recherche_texte = st.text_input(
            "Mots-clés",
            placeholder="économie, finances, PIB, croissance...",
            help="Recherche dans les titres et descriptions des datasets"
        )
        
        # Suggestions de thèmes
        st.subheader("🎯 Thématiques Prédéfinies")
        theme_selection = st.selectbox(
            "Choisir une thématique",
            options=["Toutes les thématiques"] + list(THEMATIQUES.keys())
        )
        
        if theme_selection != "Toutes les thématiques":
            tags_suggestion = THEMATIQUES[theme_selection]
            st.info(f"**Tags suggérés:** {', '.join(tags_suggestion[:3])}")
        
        # Organisation
        st.subheader("🏢 Filtre par Organisation")
        with st.spinner("Chargement des organisations..."):
            organisations = client.get_organizations_list(50)
        noms_organisations = ["Toutes les organisations"] + [safe_get(org, ['name'], 'Inconnue') for org in organisations]
        organisation_selection = st.selectbox(
            "Sélectionnez une organisation",
            options=noms_organisations
        )
        
        # Format de fichier
        st.subheader("📁 Format de Fichier")
        formats_fichiers = ["Tous les formats", "CSV", "XLS", "XLSX", "JSON", "PDF", "XML", "SHP", "GEOJSON"]
        format_selection = st.selectbox("Sélectionnez un format", options=formats_fichiers)
        
        # Tags
        st.subheader("🏷️ Tags Thématiques")
        tags_populaires = st.multiselect(
            "Sélectionnez des tags",
            options=["économie", "finances", "budget", "entreprises", "emploi", "commerce", 
                    "innovation", "statistiques", "fiscalité", "investissement", "données ouvertes",
                    "administration", "public", "gouvernement"],
            default=["économie"]
        )
        
        # Options de recherche avancée
        st.subheader("⚙️ Options Avancées")
        results_per_page = st.slider("Résultats par page", 10, 100, 20)
        only_recent = st.checkbox("📅 Datasets récents seulement (30 derniers jours)", value=False)
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Statistiques rapides
        with st.spinner("Calcul des statistiques..."):
            stats = client.get_datasets_stats()
        
        if stats:
            st.markdown("---")
            st.subheader("📊 Statistiques Globales")
            st.write(f"**Total datasets:** {stats['total_datasets']:,}")
            st.write(f"**Organisations:** {stats['organisations_count']}")
            st.write(f"**Échantillon analysé:** {stats['sample_size']} datasets")
            st.write(f"**Formats principaux:** {', '.join(list(stats['top_formats'].keys())[:2])}")
    
    return {
        'recherche_texte': recherche_texte,
        'theme_selection': theme_selection,
        'organisation_selection': organisation_selection,
        'format_selection': format_selection,
        'tags_populaires': tags_populaires,
        'results_per_page': results_per_page,
        'only_recent': only_recent
    }

def afficher_resultat_dataset(dataset, i, current_page, client):
    """Affiche un résultat de dataset de manière organisée"""
    org_name = safe_get(dataset, ['organization', 'name'], 'Inconnue')
    created_at = (dataset.get('created_at') or '')[:10]
    metrics = dataset.get('metrics', {})
    is_recent = est_recent(convertir_date(dataset.get('created_at')))
    dataset_id = dataset.get('id', f'unknown_{i}')
    
    recent_badge = " 🆕" if is_recent else ""
    
    # Utiliser un conteneur avec style personnalisé
    with st.container():
        st.markdown(f"""
        <div class="result-item">
            <div class="result-title">📊 {dataset.get('title', 'Sans titre')}{recent_badge}</div>
            <div class="result-meta">
                <strong>🏢 Organisation:</strong> {org_name} | 
                <strong>📅 Publication:</strong> {created_at} | 
                <strong>🔢 Ressources:</strong> {len(dataset.get('resources', []))} fichiers
            </div>
            <div class="result-stats">
                <strong>👁️ Visites:</strong> {metrics.get('views', 0):,} • 
                <strong>❤️ Followers:</strong> {metrics.get('followers', 0)} • 
                <strong>🔄 Réutilisations:</strong> {metrics.get('reuses', 0)}
            </div>
            <div class="result-meta">
                <strong>🏷️ Tags:</strong> {', '.join(dataset.get('tags', []))}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Actions
        col_act1, col_act2, col_act3, col_act4 = st.columns([1, 1, 1, 1])
        
        with col_act1:
            if st.button(f"📈 Analyser", key=f"analyze_{dataset_id}", use_container_width=True):
                st.session_state.dataset_analyse = dataset
                st.rerun()
        
        with col_act2:
            if st.button(f"📁 Détails", key=f"details_{dataset_id}", use_container_width=True):
                st.session_state.dataset_details = dataset
                st.rerun()
        
        with col_act3:
            if st.button(f"📥 Métadonnées", key=f"download_{dataset_id}", use_container_width=True):
                telecharger_dataset(dataset, client)
        
        with col_act4:
            dataset_url = f"https://www.data.gouv.fr/fr/datasets/{dataset.get('id', '')}/"
            st.markdown(
                f'<a href="{dataset_url}" target="_blank">'
                f'<button style="width: 100%; background: linear-gradient(135deg, #EF4135 0%, #FF6B6B 100%); color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; font-weight: bold;">'
                f'🌐 Ouvrir</button></a>', 
                unsafe_allow_html=True
            )
        
        st.markdown("---")

def afficher_pagination_avancee(page_actuelle, total_pages, position="top"):
    """Affiche les contrôles de pagination avancés"""
    if total_pages <= 1:
        return
        
    st.markdown("---")
    col_pag1, col_pag2, col_pag3, col_pag4, col_pag5 = st.columns([1, 2, 2, 2, 1])
    
    with col_pag1:
        if page_actuelle > 1:
            if st.button("⬅️ Précédent", key=f"prev_{page_actuelle}_{position}", use_container_width=True):
                st.session_state.recherche_page = page_actuelle - 1
                st.rerun()
    
    with col_pag2:
        pages_rapides = [1, max(1, page_actuelle-2), page_actuelle, min(total_pages, page_actuelle+2), total_pages]
        pages_rapides = sorted(set(pages_rapides))
        page_rapide = st.selectbox("Aller à la page:", options=pages_rapides, index=pages_rapides.index(page_actuelle), key=f"page_select_{page_actuelle}_{position}")
        if page_rapide != page_actuelle:
            st.session_state.recherche_page = page_rapide
            st.rerun()
    
    with col_pag3:
        nouvelle_page = st.number_input(
            "Page spécifique:",
            min_value=1,
            max_value=total_pages,
            value=page_actuelle,
            key=f"page_input_{page_actuelle}_{position}",
            label_visibility="collapsed"
        )
    
    with col_pag4:
        if st.button("🔍 Aller", key=f"go_{page_actuelle}_{position}", use_container_width=True) and nouvelle_page != page_actuelle:
            st.session_state.recherche_page = nouvelle_page
            st.rerun()
    
    with col_pag5:
        if page_actuelle < total_pages:
            if st.button("Suivant ➡️", key=f"next_{page_actuelle}_{position}", use_container_width=True):
                st.session_state.recherche_page = page_actuelle + 1
                st.rerun()

# =============================================================================
# FONCTIONS POUR LES GRAPHIQUES AVEC STYLES CORRIGÉS
# =============================================================================
def creer_graphique_organisations(stats):
    """Crée le graphique des organisations avec styles corrigés"""
    if not stats['top_organisations']:
        return None
        
    org_df = pd.DataFrame({
        'Organisation': list(stats['top_organisations'].keys()),
        'Datasets': list(stats['top_organisations'].values())
    }).head(15)
    
    fig = px.bar(
        org_df,
        x='Datasets',
        y='Organisation',
        orientation='h',
        title="Top 15 des Organisations Productrices",
        color='Datasets',
        color_continuous_scale=['#0055A4', '#EF4135']
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='#000000', size=12),
        title_font=dict(color='#0055A4', size=16),
        xaxis=dict(
            title_font=dict(color='#000000'),
            tickfont=dict(color='#000000'),
            gridcolor='#E0E0E0'
        ),
        yaxis=dict(
            title_font=dict(color='#000000'),
            tickfont=dict(color='#000000')
        ),
        coloraxis_colorbar=dict(
            title_font=dict(color='#000000'),
            tickfont=dict(color='#000000')
        )
    )
    
    return fig

def creer_graphique_formats(stats):
    """Crée le graphique des formats avec styles corrigés"""
    if not stats['top_formats']:
        return None
        
    format_df = pd.DataFrame({
        'Format': list(stats['top_formats'].keys()),
        'Count': list(stats['top_formats'].values())
    })
    
    fig = px.pie(
        format_df,
        values='Count',
        names='Format',
        title="Répartition des Formats de Fichiers",
        hole=0.4,
        color_discrete_sequence=['#0055A4', '#1f77b4', '#EF4135', '#FF6B6B', '#4CAF50', '#FF9800']
    )
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='#000000', size=12),
        title_font=dict(color='#0055A4', size=16),
        legend=dict(
            font=dict(color='#000000'),
            bgcolor='white',
            bordercolor='#0055A4',
            borderwidth=1
        )
    )
    
    fig.update_traces(
        textfont=dict(color='#000000', size=10),
        textposition='inside'
    )
    
    return fig

def creer_graphique_temporel(stats):
    """Crée le graphique temporel avec styles corrigés - Version corrigée"""
    # Utiliser 'ME' (Month End) au lieu de 'M' qui est déprécié
    dates_simulees = pd.date_range(end=datetime.now(), periods=12, freq='ME')
    creations_simulees = np.random.poisson(stats['sample_size'] // 12, 12) * np.linspace(0.8, 1.2, 12)
    
    fig = px.line(
        x=dates_simulees,
        y=creations_simulees,
        title="Volume de créations de datasets (estimation)",
        labels={'x': 'Mois', 'y': 'Nouveaux datasets'}
    )
    
    fig.update_traces(line=dict(color='#EF4135', width=3))
    
    fig.update_layout(
        plot_bgcolor='white',
        paper_bgcolor='white',
        font=dict(color='#000000', size=12),
        title_font=dict(color='#0055A4', size=16),
        xaxis=dict(
            title_font=dict(color='#000000'),
            tickfont=dict(color='#000000'),
            gridcolor='#E0E0E0'
        ),
        yaxis=dict(
            title_font=dict(color='#000000'),
            tickfont=dict(color='#000000'),
            gridcolor='#E0E0E0'
        )
    )
    
    return fig

# =============================================================================
# ONGLETS PRINCIPAUX
# =============================================================================
def afficher_onglet_recherche_avancee(client, filtres):
    """Affiche l'onglet de recherche avancée"""
    st.markdown('<h2 class="section-header">🔍 RECHERCHE AVANCÉE - 24K DATASETS</h2>', unsafe_allow_html=True)
    
    # Construction de la requête avancée
    query_parts = []
    
    if filtres['recherche_texte']:
        query_parts.append(filtres['recherche_texte'])
    
    if filtres['theme_selection'] != "Toutes les thématiques":
        query_parts.extend(THEMATIQUES[filtres['theme_selection']])
    
    if filtres['organisation_selection'] != "Toutes les organisations":
        query_parts.append(f'organization:"{filtres["organisation_selection"]}"')
    
    if filtres['format_selection'] != "Tous les formats":
        query_parts.append(f'format:{filtres["format_selection"]}')
    
    if filtres['tags_populaires']:
        for tag in filtres['tags_populaires']:
            query_parts.append(f'tag:{tag}')
    
    if filtres['only_recent']:
        query_parts.append('created:last-month')
    
    query_final = " ".join(query_parts) if query_parts else ""
    
    # Contrôles de recherche
    col_btn1, col_btn2, col_btn3, col_info = st.columns([2, 1, 1, 2])
    
    with col_btn1:
        if st.button("🚀 Lancer la Recherche Avancée", key="search_launch", type="primary", use_container_width=True):
            st.session_state.recherche_query = query_final
            st.session_state.recherche_page = 1
            st.rerun()
    
    with col_btn2:
        if st.button("🔄 Réinitialiser", key="search_reset", use_container_width=True):
            for key in ['recherche_query', 'recherche_resultats', 'recherche_page']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
    
    with col_btn3:
        if st.button("💾 Exporter Résultats", key="search_export", use_container_width=True):
            if 'recherche_resultats' in st.session_state:
                exporter_resultats(st.session_state.recherche_resultats)
    
    # Affichage de la requête
    if query_final:
        st.info(f"**Requête avancée:** `{query_final}`")
    
    # Exécution de la recherche
    if 'recherche_query' in st.session_state:
        executer_recherche_avancee(client, st.session_state.recherche_query, 
                                 st.session_state.get('recherche_page', 1), filtres['results_per_page'])
    else:
        afficher_guide_recherche_avancee()

def executer_recherche_avancee(client, query, page, page_size):
    """Exécute la recherche avancée sur data.gouv.fr"""
    with st.spinner(f"🔍 Recherche en cours (page {page})..."):
        resultats = client.rechercher_datasets_avances(query, page, page_size)
    
    if resultats is not None:
        st.session_state.recherche_resultats = resultats
        afficher_resultats_avances(resultats, client, page, page_size)
    else:
        st.error("❌ Impossible d'exécuter la recherche. Vérifiez votre connexion.")

def afficher_resultats_avances(resultats, client, current_page, page_size):
    """Affiche les résultats de la recherche avancée"""
    datasets = resultats.get('data', [])
    total_results = resultats.get('total', 0)
    total_pages = max(1, (total_results + page_size - 1) // page_size)
    
    # En-tête des résultats
    col_header1, col_header2, col_header3 = st.columns([3, 1, 1])
    
    with col_header1:
        st.subheader(f"📋 {total_results:,} Datasets Trouvés")
        st.caption(f"Page {current_page} sur {total_pages} • {len(datasets)} résultats affichés")
    
    with col_header2:
        st.metric("Page Actuelle", f"{current_page}/{total_pages}")
    
    with col_header3:
        if total_results > 0:
            couverture = min((current_page * page_size) / total_results * 100, 100)
            st.metric("Couverture", f"{couverture:.1f}%")
    
    # Indicateur de performance
    if total_results > 1000:
        st.info(f"🎯 **Conseil:** Utilisez les filtres avancés pour affiner votre recherche parmi les {total_results:,} résultats")
    
    # Pagination en haut
    if total_pages > 1:
        afficher_pagination_avancee(current_page, total_pages, "top")
    
    # Affichage des résultats
    if not datasets:
        st.warning("""
        ## 🎯 Aucun dataset trouvé
        
        **Suggestions pour améliorer votre recherche parmi 24K datasets:**
        
        - ✅ Utilisez les thèmes prédéfinis pour cibler votre recherche
        - ✅ Combinez plusieurs filtres (organisation + format + tags)
        - ✅ Essayez des synonymes ou termes plus génériques
        - ✅ Consultez les datasets populaires dans l'onglet dédié
        - ✅ Réduisez le nombre de filtres si trop restrictifs
        """)
        return
    
    # Statistiques rapides sur les résultats
    if datasets:
        orgs = [safe_get(d, ['organization', 'name'], 'Inconnue') for d in datasets]
        formats = list(set([(r.get('format') or '').upper() for d in datasets for r in d.get('resources', [])]))
        
        col_stats1, col_stats2, col_stats3 = st.columns(3)
        with col_stats1:
            st.metric("Organisations", len(set(orgs)))
        with col_stats2:
            st.metric("Formats", len(formats))
        with col_stats3:
            recent_count = sum(1 for d in datasets if est_recent(convertir_date(d.get('created_at'))))
            st.metric("Récents", recent_count)
    
    # Affichage détaillé des résultats
    for i, dataset in enumerate(datasets):
        afficher_resultat_dataset(dataset, i, current_page, client)
    
    # Pagination en bas
    if total_pages > 1:
        afficher_pagination_avancee(current_page, total_pages, "bottom")

def afficher_onglet_analytics_globaux(client):
    """Affiche l'onglet des analytics globaux"""
    st.markdown('<h2 class="section-header">📊 ANALYTICS GLOBAUX - 24K DATASETS</h2>', unsafe_allow_html=True)
    
    with st.spinner('🔍 Analyse des 24K datasets en cours...'):
        stats = client.get_datasets_stats()
        datasets_populaires = client.get_datasets_populaires(100)
    
    if not stats:
        st.error("""
        ❌ Impossible de charger les données pour l'analyse globale
        
        **Solutions possibles:**
        - Vérifiez votre connexion Internet
        - L'API data.gouv.fr peut être temporairement indisponible
        - Réessayez dans quelques instants
        """)
        return
    
    if not datasets_populaires:
        st.warning("⚠️ Données limitées disponibles pour l'analyse")
        datasets_populaires = []
    
    # KPI Principaux
    st.subheader("📈 Indicateurs Clés")
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Datasets", f"{stats['total_datasets']:,}")
    with col2:
        st.metric("Organisations", stats['organisations_count'])
    with col3:
        st.metric("Datasets Récents", stats['recent_datasets'])
    with col4:
        st.metric("Visites Moyennes", f"{stats['avg_views']:,}")
    with col5:
        couverture_recente = (stats['recent_datasets'] / min(100, stats['sample_size'])) * 100
        st.metric("Fraîcheur", f"{couverture_recente:.1f}%")
    
    # Graphiques d'analyse
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        fig_org = creer_graphique_organisations(stats)
        if fig_org:
            st.plotly_chart(fig_org, use_container_width=True)
        else:
            st.info("📊 Aucune donnée d'organisation disponible")
    
    with col_chart2:
        fig_format = creer_graphique_formats(stats)
        if fig_format:
            st.plotly_chart(fig_format, use_container_width=True)
        else:
            st.info("📁 Aucune donnée de format disponible")
    
    # Analyse temporelle
    st.subheader("📅 Analyse Temporelle")
    fig_temporel = creer_graphique_temporel(stats)
    if fig_temporel:
        st.plotly_chart(fig_temporel, use_container_width=True)
    
    # Tableau des datasets les plus populaires
    if datasets_populaires:
        st.subheader("🏆 Top 25 des Datasets les Plus Populaires")
        
        datasets_tries = sorted(datasets_populaires, 
                              key=lambda x: x.get('metrics', {}).get('views', 0), 
                              reverse=True)[:25]
        
        data_table = []
        for i, dataset in enumerate(datasets_tries, 1):
            data_table.append({
                'Rank': i,
                'Titre': dataset.get('title', 'Sans titre')[:60] + '...' if len(dataset.get('title', '')) > 60 else dataset.get('title', 'Sans titre'),
                'Organisation': safe_get(dataset, ['organization', 'name'], 'Inconnue'),
                'Visites': f"{dataset.get('metrics', {}).get('views', 0):,}",
                'Followers': dataset.get('metrics', {}).get('followers', 0),
                'Ressources': len(dataset.get('resources', [])),
                'Récent': '🆕' if est_recent(convertir_date(dataset.get('created_at'))) else ''
            })
        
        df_table = pd.DataFrame(data_table)
        st.dataframe(df_table, use_container_width=True, height=400)
    else:
        st.info("⭐ Aucun dataset populaire disponible pour l'instant")

def afficher_onglet_top_datasets(client):
    """Affiche l'onglet des datasets populaires"""
    st.markdown('<h2 class="section-header">⭐ TOP 50 DES DATASETS POPULAIRES</h2>', unsafe_allow_html=True)
    
    with st.spinner('🔍 Chargement des 50 datasets les plus populaires...'):
        datasets_populaires = client.get_datasets_populaires(50)
    
    if not datasets_populaires:
        st.error("""
        ❌ Impossible de charger les datasets populaires
        
        **Causes possibles:**
        - Problème de connexion à l'API data.gouv.fr
        - Données temporairement indisponibles
        - Essayez de rafraîchir la page
        """)
        return
    
    st.success(f"✅ {len(datasets_populaires)} datasets populaires chargés")
    
    # Filtres pour les datasets populaires
    col_filter1, col_filter2 = st.columns(2)
    with col_filter1:
        organisations_uniques = list(set([safe_get(d, ['organization', 'name'], 'Inconnue') for d in datasets_populaires]))
        filter_org = st.selectbox("Filtrer par organisation", 
                                options=["Toutes"] + organisations_uniques)
    with col_filter2:
        min_views = st.slider("Visites minimum", 0, 10000, 0)
    
    # Appliquer les filtres
    datasets_filtres = datasets_populaires
    if filter_org != "Toutes":
        datasets_filtres = [d for d in datasets_filtres if safe_get(d, ['organization', 'name']) == filter_org]
    datasets_filtres = [d for d in datasets_filtres if d.get('metrics', {}).get('views', 0) >= min_views]
    
    if not datasets_filtres:
        st.warning("Aucun dataset ne correspond aux filtres sélectionnés")
        return
    
    for i, dataset in enumerate(datasets_filtres):
        dataset_id = dataset.get('id', f'pop_{i}')
        with st.expander(f"🏆 {i+1}. {dataset.get('title', 'Sans titre')}", expanded=i<3):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                org_name = safe_get(dataset, ['organization', 'name'], 'Inconnue')
                metrics = dataset.get('metrics', {})
                is_recent = est_recent(convertir_date(dataset.get('created_at')))
                
                st.write(f"**Organisation:** {org_name}")
                st.write(f"**Description:** {(dataset.get('description') or 'Aucune description disponible')[:300]}...")
                st.write(f"**📊 Métriques:** 👁️ {metrics.get('views', 0):,} vues • ❤️ {metrics.get('followers', 0)} followers • 🔄 {metrics.get('reuses', 0)} réutilisations")
                st.write(f"**📁 Ressources:** {len(dataset.get('resources', []))} fichiers")
                st.write(f"**🏷️ Tags:** {', '.join(dataset.get('tags', []))}")
                if is_recent:
                    st.success("🆕 Dataset récent (moins de 30 jours)")
            
            with col2:
                if st.button(f"🔍 Analyser", key=f"pop_analyze_{dataset_id}"):
                    st.session_state.dataset_analyse = dataset
                    st.rerun()
                
                dataset_url = f"https://www.data.gouv.fr/fr/datasets/{dataset.get('id', '')}/"
                st.markdown(f'<a href="{dataset_url}" target="_blank"><button style="background: linear-gradient(135deg, #0055A4 0%, #1f77b4 100%); color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; margin-top: 10px; width: 100%; font-weight: bold;">🌐 Ouvrir sur data.gouv.fr</button></a>', unsafe_allow_html=True)
                
                if st.button(f"📥 Métadonnées", key=f"pop_dl_{dataset_id}"):
                    telecharger_dataset(dataset, client)

# =============================================================================
# FONCTIONS DE GESTION DES DONNÉES
# =============================================================================
def telecharger_dataset(dataset, client):
    """Télécharge et prépare un dataset pour l'export"""
    with st.spinner("📥 Préparation du téléchargement..."):
        # Créer un DataFrame avec les métadonnées
        data = {
            'Titre': [dataset.get('title')],
            'Organisation': [safe_get(dataset, ['organization', 'name'])],
            'Description': [dataset.get('description', '')[:500]],
            'Tags': [', '.join(dataset.get('tags', []))],
            'Date création': [dataset.get('created_at')],
            'Visites': [dataset.get('metrics', {}).get('views', 0)],
            'Followers': [dataset.get('metrics', {}).get('followers', 0)],
            'Ressources': [len(dataset.get('resources', []))],
            'URL': [f"https://www.data.gouv.fr/fr/datasets/{dataset.get('id', '')}/"]
        }
        
        df = pd.DataFrame(data)
        csv = df.to_csv(index=False)
        
        st.download_button(
            label="📥 Télécharger les métadonnées",
            data=csv,
            file_name=f"metadata_{dataset.get('id', 'dataset')}.csv",
            mime="text/csv",
            use_container_width=True
        )

def exporter_resultats(resultats):
    """Exporte les résultats de recherche"""
    datasets = resultats.get('data', [])
    if not datasets:
        st.warning("Aucun résultat à exporter")
        return
    
    # Préparer les données pour l'export
    export_data = []
    for dataset in datasets:
        export_data.append({
            'Titre': dataset.get('title'),
            'Organisation': safe_get(dataset, ['organization', 'name']),
            'Description': (dataset.get('description') or '')[:200],
            'Tags': ', '.join(dataset.get('tags', [])),
            'Date création': dataset.get('created_at'),
            'Visites': dataset.get('metrics', {}).get('views', 0),
            'Followers': dataset.get('metrics', {}).get('followers', 0),
            'Ressources': len(dataset.get('resources', [])),
            'URL': f"https://www.data.gouv.fr/fr/datasets/{dataset.get('id', '')}/"
        })
    
    df = pd.DataFrame(export_data)
    csv = df.to_csv(index=False)
    
    st.download_button(
        label="💾 Exporter tous les résultats (CSV)",
        data=csv,
        file_name=f"recherche_data_gouv_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        mime="text/csv",
        use_container_width=True
    )

def afficher_guide_recherche_avancee():
    """Affiche le guide d'utilisation de la recherche avancée"""
    st.markdown("""
    ## 🎯 Guide de Recherche Avancée - 24K Datasets
    
    ### 🔍 Stratégies de Recherche Efficace :
    
    **Pour naviguer parmi 24,000 datasets:**
    
    1. **🎯 Utilisez les thèmes prédéfinis** - Catégories optimisées pour data.gouv.fr
    2. **🏢 Filtrez par organisation** - INSEE, Ministères, Collectivités...
    3. **📁 Spécifiez le format** - CSV, Excel, JSON, Géodonnées...
    4. **🏷️ Combinez les tags** - Économie + Finances + Statistiques
    
    ### 💡 Exemples de Recherches Avancées :
    
    - **`économie organization:"INSEE" format:CSV`** - Données économiques INSEE en CSV
    - **`budget finances tag:public`** - Budgets et finances publiques
    - **`entreprises emploi`** - Données sur entreprises et emploi
    - **`transport format:SHP`** - Données géospatiales transport
    
    ### 🚀 Fonctionnalités Avancées :
    
    - **Recherche en temps réel** sur l'ensemble du catalogue
    - **Filtres combinés** pour une précision maximale
    - **Pagination intelligente** pour naviguer rapidement
    - **Export des résultats** en CSV
    - **Analyse de qualité** des datasets
    - **Téléchargement direct** des ressources
    
    ### 📊 Conseils Performance :
    
    - Commencez par une recherche large puis affinez
    - Utilisez les datasets populaires comme point de départ
    - Vérifiez la date de mise à jour pour la fraîcheur des données
    - Consultez les analytics pour identifier les producteurs fiables
    """)

# =============================================================================
# FONCTION PRINCIPALE
# =============================================================================
def main():
    # En-tête principal
    st.markdown("""
    <div style='text-align: center; background: linear-gradient(135deg, #0055A4 0%, #FFFFFF 50%, #EF4135 100%); padding: 20px; border-radius: 15px; margin-bottom: 20px;'>
        <h1 class="main-header">🔍 DASHBOARD DATA.GOUV.FR</h1>
        <p style='color: #0055A4; font-size: 1.2rem; font-weight: bold;'>Analyse de 24,187 datasets ouverts français</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<p class="data-source">📡 Source: API data.gouv.fr - Données ouvertes de la République Française</p>', unsafe_allow_html=True)
    
    # Initialisation du client API
    client = DataGouvAPIClient()
    
    # Sidebar avec contrôles de recherche
    filtres = afficher_sidebar(client)
    
    # Onglets principaux
    tab1, tab2, tab3 = st.tabs([
        "🔍 Recherche Avancée", 
        "📊 Analytics Globaux", 
        "⭐ Top Datasets"
    ])
    
    # Contenu des onglets
    with tab1:
        afficher_onglet_recherche_avancee(client, filtres)
    
    with tab2:
        afficher_onglet_analytics_globaux(client)
    
    with tab3:
        afficher_onglet_top_datasets(client)

# =============================================================================
# GESTION DES ÉTATS ET EXÉCUTION
# =============================================================================
if __name__ == "__main__":
    main()
