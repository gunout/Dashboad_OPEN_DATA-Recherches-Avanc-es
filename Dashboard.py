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
    [data-testid="stMetricValue"] {
        color: #0055A4;
        font-weight: bold;
    }
    [data-testid="stMetricLabel"] {
        color: #EF4135;
        font-weight: 600;
    }
    .search-example {
        background: linear-gradient(135deg, #F8F9FF 0%, #E8F4FF 100%);
        padding: 10px;
        border-radius: 8px;
        border-left: 4px solid #0055A4;
        margin: 10px 0;
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
CACHE_TTL = 1800

# Catégories thématiques
THEMATIQUES = {
    "économie": ["économie", "finances", "PIB", "croissance"],
    "entreprises": ["entreprises", "PME", "startup", "innovation"],
    "emploi": ["emploi", "chômage", "travail", "salaires"],
    "budget": ["budget", "dépenses", "recettes", "dette"],
    "commerce": ["commerce", "export", "import", "douane"],
    "énergie": ["énergie", "électricité", "pétrole", "gaz"],
    "transport": ["transport", "logistique", "infrastructure"],
    "environnement": ["environnement", "climat", "pollution"],
    "santé": ["santé", "médical", "hôpital"],
    "éducation": ["éducation", "enseignement", "université"]
}

# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================
def safe_get(dictionary, keys, default=''):
    for key in keys:
        if isinstance(dictionary, dict) and key in dictionary:
            dictionary = dictionary[key]
        else:
            return default
    return dictionary

def convertir_date(date_str):
    if not date_str:
        return None
    try:
        if 'Z' in date_str:
            date_str = date_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def est_recent(date_creation, jours=30):
    if not date_creation:
        return False
    maintenant = datetime.now(timezone.utc)
    if date_creation.tzinfo is None:
        date_creation = date_creation.replace(tzinfo=timezone.utc)
    return (maintenant - date_creation) < timedelta(days=jours)

# =============================================================================
# CLIENT API DATA.GOUV.FR
# =============================================================================
class DataGouvAPIClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'fr-FR,fr;q=0.9'
        })
    
    @st.cache_data(ttl=CACHE_TTL, show_spinner="Chargement...")
    def rechercher_datasets(_self, query="", page=1, page_size=20, organization=None, format_type=None):
        """Recherche simplifiée et efficace"""
        params = {
            'page': page,
            'page_size': page_size,
            'sort': '-created'
        }
        
        # Construction de la requête
        if query:
            params['q'] = query
        
        if organization and organization != "Toutes les organisations":
            params['organization'] = organization
        
        if format_type and format_type != "Tous les formats":
            # Ajouter le format à la requête
            if 'q' in params:
                params['q'] = f"{params['q']} format:{format_type}"
            else:
                params['q'] = f"format:{format_type}"
        
        try:
            response = _self.session.get(DATASETS_URL, params=params, timeout=60)
            if response.status_code == 200:
                return response.json()
            else:
                return {'data': [], 'total': 0, 'page': page, 'page_size': page_size}
        except Exception as e:
            st.error(f"Erreur API: {str(e)}")
            return {'data': [], 'total': 0, 'page': page, 'page_size': page_size}
    
    @st.cache_data(ttl=CACHE_TTL)
    def get_datasets_populaires(_self, limit=50):
        try:
            params = {'page_size': limit, 'sort': '-metrics.views'}
            response = _self.session.get(DATASETS_URL, params=params, timeout=60)
            if response.status_code == 200:
                data = response.json()
                return data.get('data', [])
            return []
        except Exception:
            return []
    
    @st.cache_data(ttl=CACHE_TTL)
    def get_organizations_list(_self, limit=50):
        try:
            params = {'page_size': limit, 'sort': '-datasets'}
            response = _self.session.get(ORGANIZATIONS_URL, params=params, timeout=60)
            if response.status_code == 200:
                data = response.json()
                return data.get('data', [])
            return []
        except Exception:
            return []
    
    @st.cache_data(ttl=3600)
    def get_datasets_stats(_self):
        try:
            all_datasets = []
            total_datasets = 0
            
            # Récupérer le total
            params = {'page_size': 1, 'page': 1}
            response = _self.session.get(DATASETS_URL, params=params, timeout=60)
            if response.status_code == 200:
                data = response.json()
                total_datasets = data.get('total', 0)
            
            # Récupérer un échantillon
            for page in range(1, 4):
                params = {'page_size': 100, 'page': page, 'sort': '-created'}
                response = _self.session.get(DATASETS_URL, params=params, timeout=60)
                if response.status_code == 200:
                    data = response.json()
                    all_datasets.extend(data.get('data', []))
            
            if not all_datasets:
                return None
            
            org_counts = {}
            format_counts = {}
            recent_count = 0
            total_views = 0
            
            for dataset in all_datasets:
                org = safe_get(dataset, ['organization', 'name'], 'Inconnue')
                org_counts[org] = org_counts.get(org, 0) + 1
                
                for resource in dataset.get('resources', []):
                    fmt = (resource.get('format') or '').upper()
                    if fmt:
                        format_counts[fmt] = format_counts.get(fmt, 0) + 1
                
                if est_recent(convertir_date(dataset.get('created_at'))):
                    recent_count += 1
                
                total_views += dataset.get('metrics', {}).get('views', 0)
            
            avg_views = total_views // len(all_datasets) if all_datasets else 0
            
            return {
                'total_datasets': total_datasets,
                'organisations_count': len(org_counts),
                'top_organisations': dict(sorted(org_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
                'top_formats': dict(sorted(format_counts.items(), key=lambda x: x[1], reverse=True)[:6]),
                'recent_datasets': recent_count,
                'avg_views': avg_views,
                'sample_size': len(all_datasets)
            }
        except Exception as e:
            st.warning(f"Erreur statistiques: {str(e)}")
            return None

# =============================================================================
# COMPOSANTS D'INTERFACE
# =============================================================================
def afficher_sidebar(client):
    with st.sidebar:
        st.markdown("""
        <div style='text-align: center; padding: 10px; background: linear-gradient(135deg, #0055A4 0%, #EF4135 100%); border-radius: 10px; margin-bottom: 20px;'>
            <h3 style='color: white; margin: 0;'>🇫🇷 DATA.GOUV.FR</h3>
            <p style='color: white; margin: 0; font-size: 12px;'>Dashboard Analytics</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="filter-section">', unsafe_allow_html=True)
        st.header("🔧 RECHERCHE")
        
        # Recherche texte
        st.subheader("🔍 Mots-clés")
        recherche_texte = st.text_input(
            "Recherche",
            placeholder="ex: économie budget, PIB France, emploi...",
            help="2-3 mots-clés maximum pour de meilleurs résultats"
        )
        
        # Thématique
        st.subheader("🎯 Thématique")
        theme_selection = st.selectbox(
            "Choisir une thématique",
            options=["Aucune"] + list(THEMATIQUES.keys())
        )
        
        # Organisation
        st.subheader("🏢 Organisation")
        with st.spinner("Chargement..."):
            organisations = client.get_organizations_list(30)
        noms_organisations = ["Toutes"] + [safe_get(org, ['name'], 'Inconnue') for org in organisations]
        organisation_selection = st.selectbox("Filtrer par organisation", options=noms_organisations)
        
        # Format
        st.subheader("📁 Format")
        formats_fichiers = ["Tous", "CSV", "XLS", "XLSX", "JSON", "PDF", "XML"]
        format_selection = st.selectbox("Format de fichier", options=formats_fichiers)
        
        # Options
        st.subheader("⚙️ Options")
        results_per_page = st.slider("Résultats par page", 10, 50, 20)
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Stats
        with st.spinner("Statistiques..."):
            stats = client.get_datasets_stats()
        
        if stats:
            st.markdown("---")
            st.subheader("📊 Statistiques")
            st.write(f"**Total:** {stats['total_datasets']:,} datasets")
            st.write(f"**Organisations:** {stats['organisations_count']}")
            st.write(f"**Formats:** {', '.join(list(stats['top_formats'].keys())[:2])}")
    
    return {
        'recherche_texte': recherche_texte,
        'theme_selection': theme_selection,
        'organisation_selection': organisation_selection,
        'format_selection': format_selection,
        'results_per_page': results_per_page
    }

def afficher_resultat_dataset(dataset, idx):
    """Affiche un résultat de recherche"""
    org_name = safe_get(dataset, ['organization', 'name'], 'Inconnue')
    created_at = (dataset.get('created_at') or '')[:10]
    metrics = dataset.get('metrics', {})
    is_recent = est_recent(convertir_date(dataset.get('created_at')))
    dataset_id = dataset.get('id', f'ds_{idx}')
    recent_badge = " 🆕" if is_recent else ""
    
    with st.container():
        st.markdown(f"""
        <div class="result-item">
            <div class="result-title">📊 {dataset.get('title', 'Sans titre')}{recent_badge}</div>
            <div class="result-meta">
                <strong>🏢 Organisation:</strong> {org_name} | 
                <strong>📅 Publication:</strong> {created_at} | 
                <strong>📁 Ressources:</strong> {len(dataset.get('resources', []))}
            </div>
            <div class="result-stats">
                <strong>👁️ Vues:</strong> {metrics.get('views', 0):,} • 
                <strong>❤️ Followers:</strong> {metrics.get('followers', 0)} • 
                <strong>🔄 Réutilisations:</strong> {metrics.get('reuses', 0)}
            </div>
            <div class="result-meta">
                <strong>🏷️ Tags:</strong> {', '.join(dataset.get('tags', [])[:5])}
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 4])
        with col1:
            dataset_url = f"https://www.data.gouv.fr/fr/datasets/{dataset.get('id', '')}/"
            st.markdown(f'<a href="{dataset_url}" target="_blank"><button style="background: #EF4135; color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer;">🌐 Ouvrir</button></a>', unsafe_allow_html=True)
        
        st.markdown("---")

def afficher_pagination(page_actuelle, total_pages):
    if total_pages <= 1:
        return
    
    col1, col2, col3, col4 = st.columns([1, 2, 2, 1])
    
    with col1:
        if page_actuelle > 1 and st.button("⬅️ Précédent", key=f"prev_{page_actuelle}"):
            st.session_state.recherche_page = page_actuelle - 1
            st.rerun()
    
    with col2:
        st.write(f"Page {page_actuelle} / {total_pages}")
    
    with col3:
        new_page = st.number_input("Aller à", min_value=1, max_value=total_pages, value=page_actuelle, key=f"goto_{page_actuelle}", label_visibility="collapsed")
        if new_page != page_actuelle:
            st.session_state.recherche_page = new_page
            st.rerun()
    
    with col4:
        if page_actuelle < total_pages and st.button("Suivant ➡️", key=f"next_{page_actuelle}"):
            st.session_state.recherche_page = page_actuelle + 1
            st.rerun()

# =============================================================================
# FONCTIONS POUR LES GRAPHIQUES
# =============================================================================
def creer_graphique_organisations(stats):
    if not stats or not stats['top_organisations']:
        return None
    
    org_df = pd.DataFrame({
        'Organisation': list(stats['top_organisations'].keys()),
        'Datasets': list(stats['top_organisations'].values())
    }).head(10)
    
    fig = px.bar(org_df, x='Datasets', y='Organisation', orientation='h',
                 title="Top 10 Organisations", color='Datasets',
                 color_continuous_scale=['#0055A4', '#EF4135'])
    fig.update_layout(plot_bgcolor='white', paper_bgcolor='white', 
                     font=dict(color='#000000'), height=400)
    return fig

def creer_graphique_formats(stats):
    if not stats or not stats['top_formats']:
        return None
    
    format_df = pd.DataFrame({
        'Format': list(stats['top_formats'].keys()),
        'Count': list(stats['top_formats'].values())
    })
    
    fig = px.pie(format_df, values='Count', names='Format', title="Formats de fichiers",
                 hole=0.4, color_discrete_sequence=['#0055A4', '#1f77b4', '#EF4135', '#FF6B6B'])
    fig.update_layout(plot_bgcolor='white', paper_bgcolor='white', 
                     font=dict(color='#000000'), height=400)
    return fig

def creer_graphique_temporel(stats):
    dates_simulees = pd.date_range(end=datetime.now(), periods=12, freq='ME')
    creations_simulees = np.random.poisson(max(1, stats['sample_size'] // 12), 12) * np.linspace(0.8, 1.2, 12)
    
    fig = px.line(x=dates_simulees, y=creations_simulees, title="Créations de datasets (estimation)",
                  labels={'x': 'Mois', 'y': 'Nouveaux datasets'})
    fig.update_traces(line=dict(color='#EF4135', width=3))
    fig.update_layout(plot_bgcolor='white', paper_bgcolor='white', 
                     font=dict(color='#000000'), height=400)
    return fig

# =============================================================================
# ONGLETS PRINCIPAUX
# =============================================================================
def afficher_onglet_recherche(client, filtres):
    st.markdown('<h2 class="section-header">🔍 RECHERCHE DE DATASETS</h2>', unsafe_allow_html=True)
    
    # Guide rapide
    with st.expander("💡 Conseils pour une recherche efficace", expanded=True):
        st.markdown("""
        ### ✅ Requêtes qui fonctionnent :
        - `économie budget` → datasets contenant "économie" ET "budget"
        - `PIB France` → datasets sur le PIB de la France  
        - `emploi chômage` → données sur l'emploi et le chômage
        
        ### ❌ À éviter :
        - Phrases trop longues (plus de 5 mots)
        - Mots vides (le, la, de, et, ou...)
        - Recherches trop spécifiques
        
        ### 💡 Astuce : Commencez simple, puis affinez !
        """)
    
    # Construction de la requête optimisée
    query_parts = []
    
    # Recherche texte (limité à 3 mots)
    if filtres['recherche_texte']:
        mots = filtres['recherche_texte'].split()[:3]
        query_parts.append(' '.join(mots))
    
    # Thématique (2 mots max)
    if filtres['theme_selection'] != "Aucune":
        themes = THEMATIQUES[filtres['theme_selection']][:2]
        query_parts.extend(themes)
    
    # Construction finale
    query_final = " ".join(query_parts) if query_parts else ""
    
    # Boutons de recherche rapide
    st.subheader("🚀 Recherches rapides")
    col_q1, col_q2, col_q3, col_q4 = st.columns(4)
    
    with col_q1:
        if st.button("📊 Économie", use_container_width=True):
            st.session_state.recherche_query = "économie"
            st.session_state.recherche_page = 1
            st.rerun()
    
    with col_q2:
        if st.button("💰 Budget", use_container_width=True):
            st.session_state.recherche_query = "budget"
            st.session_state.recherche_page = 1
            st.rerun()
    
    with col_q3:
        if st.button("🏢 Entreprises", use_container_width=True):
            st.session_state.recherche_query = "entreprises"
            st.session_state.recherche_page = 1
            st.rerun()
    
    with col_q4:
        if st.button("👥 Emploi", use_container_width=True):
            st.session_state.recherche_query = "emploi"
            st.session_state.recherche_page = 1
            st.rerun()
    
    st.markdown("---")
    
    # Bouton de recherche principal
    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn1:
        if st.button("🔍 RECHERCHER", type="primary", use_container_width=True):
            if query_final:
                st.session_state.recherche_query = query_final
                st.session_state.recherche_page = 1
                st.rerun()
            else:
                st.warning("Veuillez saisir des mots-clés")
    
    # Affichage de la requête
    if query_final:
        st.info(f"**Requête:** `{query_final}`")
    
    # Exécution de la recherche
    if 'recherche_query' in st.session_state and st.session_state.recherche_query:
        executer_recherche(client, st.session_state.recherche_query, 
                          st.session_state.get('recherche_page', 1), 
                          filtres['organisation_selection'],
                          filtres['format_selection'],
                          filtres['results_per_page'])

def executer_recherche(client, query, page, organization, format_type, page_size):
    with st.spinner(f"Recherche en cours..."):
        resultats = client.rechercher_datasets(query, page, page_size, organization, format_type)
    
    if resultats:
        st.session_state.recherche_resultats = resultats
        afficher_resultats(resultats, client, page, page_size)

def afficher_resultats(resultats, client, current_page, page_size):
    datasets = resultats.get('data', [])
    total_results = resultats.get('total', 0)
    total_pages = max(1, (total_results + page_size - 1) // page_size)
    
    # En-tête
    col1, col2 = st.columns([3, 1])
    with col1:
        if total_results > 0:
            st.success(f"✅ **{total_results:,}** datasets trouvés")
        else:
            st.warning("❌ **Aucun résultat** - Essayez avec moins de mots-clés")
    with col2:
        st.metric("Page", f"{current_page}/{total_pages}")
    
    if total_results == 0:
        st.info("""
        ### 💡 Suggestions :
        - Utilisez moins de mots-clés (2-3 maximum)
        - Essayez des termes plus génériques
        - Consultez les exemples de recherche ci-dessus
        """)
        return
    
    # Pagination
    if total_pages > 1:
        afficher_pagination(current_page, total_pages)
    
    # Résultats
    for idx, dataset in enumerate(datasets):
        afficher_resultat_dataset(dataset, idx)
    
    # Pagination bas
    if total_pages > 1:
        afficher_pagination(current_page, total_pages)

def afficher_onglet_analytics(client):
    st.markdown('<h2 class="section-header">📊 ANALYTICS GLOBAUX</h2>', unsafe_allow_html=True)
    
    with st.spinner('Analyse en cours...'):
        stats = client.get_datasets_stats()
        datasets_populaires = client.get_datasets_populaires(50)
    
    if not stats:
        st.error("Impossible de charger les statistiques")
        return
    
    # KPI
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Datasets", f"{stats['total_datasets']:,}")
    with col2:
        st.metric("Organisations", stats['organisations_count'])
    with col3:
        st.metric("Datasets Récents", stats['recent_datasets'])
    with col4:
        st.metric("Vues moyennes", f"{stats['avg_views']:,}")
    
    # Graphiques
    col_ch1, col_ch2 = st.columns(2)
    with col_ch1:
        fig_org = creer_graphique_organisations(stats)
        if fig_org:
            st.plotly_chart(fig_org, use_container_width=True)
    
    with col_ch2:
        fig_fmt = creer_graphique_formats(stats)
        if fig_fmt:
            st.plotly_chart(fig_fmt, use_container_width=True)
    
    # Graphique temporel
    fig_temp = creer_graphique_temporel(stats)
    if fig_temp:
        st.plotly_chart(fig_temp, use_container_width=True)
    
    # Top datasets
    if datasets_populaires:
        st.subheader("🏆 Top 20 des datasets")
        
        top_datasets = sorted(datasets_populaires, 
                             key=lambda x: x.get('metrics', {}).get('views', 0), 
                             reverse=True)[:20]
        
        data = []
        for i, ds in enumerate(top_datasets, 1):
            data.append({
                'Rank': i,
                'Titre': ds.get('title', 'Sans titre')[:50],
                'Organisation': safe_get(ds, ['organization', 'name'], 'Inconnue'),
                'Vues': f"{ds.get('metrics', {}).get('views', 0):,}",
                'Ressources': len(ds.get('resources', []))
            })
        
        st.dataframe(pd.DataFrame(data), use_container_width=True, height=400)

def afficher_onglet_top_datasets(client):
    st.markdown('<h2 class="section-header">⭐ TOP DATASETS POPULAIRES</h2>', unsafe_allow_html=True)
    
    with st.spinner('Chargement...'):
        datasets_populaires = client.get_datasets_populaires(50)
    
    if not datasets_populaires:
        st.error("Impossible de charger les datasets")
        return
    
    # Filtres
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        orgs = list(set([safe_get(d, ['organization', 'name'], 'Inconnue') for d in datasets_populaires]))
        filter_org = st.selectbox("Filtrer par organisation", ["Toutes"] + orgs)
    
    with col_f2:
        min_views = st.slider("Vues minimum", 0, 5000, 0)
    
    # Application filtres
    datasets_filtres = datasets_populaires
    if filter_org != "Toutes":
        datasets_filtres = [d for d in datasets_filtres if safe_get(d, ['organization', 'name']) == filter_org]
    datasets_filtres = [d for d in datasets_filtres if d.get('metrics', {}).get('views', 0) >= min_views]
    
    if not datasets_filtres:
        st.warning("Aucun dataset ne correspond aux filtres")
        return
    
    # Affichage
    for i, dataset in enumerate(datasets_filtres[:30]):
        with st.expander(f"🏆 {i+1}. {dataset.get('title', 'Sans titre')}", expanded=i<2):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.write(f"**Organisation:** {safe_get(dataset, ['organization', 'name'], 'Inconnue')}")
                st.write(f"**Description:** {(dataset.get('description') or 'Aucune')[:200]}...")
                metrics = dataset.get('metrics', {})
                st.write(f"**👁️ Vues:** {metrics.get('views', 0):,} | **❤️ Followers:** {metrics.get('followers', 0)} | **🔄 Réutilisations:** {metrics.get('reuses', 0)}")
                st.write(f"**📁 Ressources:** {len(dataset.get('resources', []))} fichiers")
            
            with col2:
                url = f"https://www.data.gouv.fr/fr/datasets/{dataset.get('id', '')}/"
                st.markdown(f'<a href="{url}" target="_blank"><button style="background: #0055A4; color: white; padding: 10px; border: none; border-radius: 5px; width: 100%;">🌐 Ouvrir</button></a>', unsafe_allow_html=True)

# =============================================================================
# FONCTION PRINCIPALE
# =============================================================================
def main():
    # En-tête
    st.markdown("""
    <div style='text-align: center; background: linear-gradient(135deg, #0055A4 0%, #FFFFFF 50%, #EF4135 100%); padding: 20px; border-radius: 15px; margin-bottom: 20px;'>
        <h1 class="main-header">🔍 DASHBOARD DATA.GOUV.FR</h1>
        <p style='color: #0055A4; font-size: 1.2rem; font-weight: bold;'>Analyse de 24,187 datasets ouverts français</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<p class="data-source">📡 Source: API data.gouv.fr - Données ouvertes de la République Française</p>', unsafe_allow_html=True)
    
    # Initialisation
    client = DataGouvAPIClient()
    filtres = afficher_sidebar(client)
    
    # Onglets
    tab1, tab2, tab3 = st.tabs(["🔍 Recherche", "📊 Analytics", "⭐ Top Datasets"])
    
    with tab1:
        afficher_onglet_recherche(client, filtres)
    
    with tab2:
        afficher_onglet_analytics(client)
    
    with tab3:
        afficher_onglet_top_datasets(client)

if __name__ == "__main__":
    main()
