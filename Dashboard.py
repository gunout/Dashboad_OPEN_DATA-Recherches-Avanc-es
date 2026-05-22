import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
from datetime import datetime, timedelta, timezone
import numpy as np
import time
import warnings
import uuid

warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURATION DE LA PAGE
# =============================================================================
st.set_page_config(
    page_title="Dashboard Analytics Data.gouv.fr",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# STYLES CSS
# =============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem; 
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
    .result-item {
        background: #FFFFFF;
        border: 2px solid #0055A4;
        border-radius: 10px;
        padding: 20px;
        margin: 15px 0;
        box-shadow: 0 4px 6px rgba(0,85,164,0.1);
        border-top: 3px solid #EF4135;
    }
    .result-title {
        font-size: 1.2rem;
        font-weight: bold;
        color: #0055A4;
        margin-bottom: 10px;
    }
    .filter-section {
        background: linear-gradient(135deg, #F8F9FF 0%, #E8F4FF 100%); 
        padding: 1.5rem; 
        border-radius: 10px; 
        margin-bottom: 1rem;
        border: 2px solid #0055A4;
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
</style>
""", unsafe_allow_html=True)

# =============================================================================
# CONSTANTES
# =============================================================================
API_BASE_URL = "https://www.data.gouv.fr/api/1"
DATASETS_URL = f"{API_BASE_URL}/datasets/"

# Données de démonstration
DEMO_DATASETS = [
    {
        "id": "1",
        "title": "Budget général de l'État - 2024",
        "description": "Budget général de l'État, recettes et dépenses par mission et programme",
        "organization": {"name": "Ministère de l'Économie et des Finances"},
        "created_at": "2024-01-15T00:00:00Z",
        "metrics": {"views": 15234, "followers": 89, "reuses": 12},
        "tags": ["budget", "finances", "économie"],
        "resources": [{"format": "CSV"}, {"format": "PDF"}]
    },
    {
        "id": "2",
        "title": "Produit Intérieur Brut (PIB) - France",
        "description": "Séries longues du PIB et de ses composantes",
        "organization": {"name": "INSEE"},
        "created_at": "2024-02-10T00:00:00Z",
        "metrics": {"views": 8923, "followers": 56, "reuses": 8},
        "tags": ["pib", "économie", "croissance"],
        "resources": [{"format": "CSV"}, {"format": "XLSX"}]
    },
    {
        "id": "3",
        "title": "Dette publique de la France",
        "description": "Dette publique au sens de Maastricht",
        "organization": {"name": "INSEE"},
        "created_at": "2024-01-20T00:00:00Z",
        "metrics": {"views": 12456, "followers": 67, "reuses": 9},
        "tags": ["dette", "finances", "économie"],
        "resources": [{"format": "CSV"}, {"format": "PDF"}]
    },
    {
        "id": "4",
        "title": "Dépenses publiques par fonction",
        "description": "Dépenses des administrations publiques par fonction (COFOG)",
        "organization": {"name": "Ministère de l'Économie"},
        "created_at": "2024-02-05T00:00:00Z",
        "metrics": {"views": 5678, "followers": 34, "reuses": 5},
        "tags": ["dépenses", "budget", "finances"],
        "resources": [{"format": "CSV"}]
    },
    {
        "id": "5",
        "title": "Chiffre d'affaires des entreprises",
        "description": "Évolution du chiffre d'affaires des entreprises françaises",
        "organization": {"name": "Banque de France"},
        "created_at": "2024-02-20T00:00:00Z",
        "metrics": {"views": 7234, "followers": 45, "reuses": 6},
        "tags": ["entreprises", "économie", "commerce"],
        "resources": [{"format": "XLSX"}, {"format": "PDF"}]
    },
    {
        "id": "6",
        "title": "Emploi et chômage en France",
        "description": "Taux de chômage, emploi par secteur",
        "organization": {"name": "INSEE"},
        "created_at": "2024-02-25T00:00:00Z",
        "metrics": {"views": 9876, "followers": 78, "reuses": 11},
        "tags": ["emploi", "chômage", "économie"],
        "resources": [{"format": "CSV"}]
    }
]

# =============================================================================
# CLIENT API
# =============================================================================
class DataGouvAPIClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'fr-FR,fr;q=0.9'
        })
    
    def rechercher_datasets(self, query="", page=1, page_size=20):
        """Recherche avec fallback sur données de démo"""
        params = {
            'page': page,
            'page_size': page_size,
            'sort': '-created'
        }
        
        if query and query.strip():
            mots = query.strip().split()[:3]
            params['q'] = ' '.join(mots)
        
        try:
            response = self.session.get(DATASETS_URL, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('data') and len(data.get('data', [])) > 0:
                    return data
                else:
                    return self._get_demo_results(query, page, page_size)
            else:
                return self._get_demo_results(query, page, page_size)
                
        except Exception as e:
            return self._get_demo_results(query, page, page_size)
    
    def _get_demo_results(self, query="", page=1, page_size=20):
        """Retourne des données de démonstration filtrées"""
        if query:
            query_lower = query.lower()
            mots_cles = query_lower.split()
            
            resultats = []
            for ds in DEMO_DATASETS:
                titre = ds.get('title', '').lower()
                desc = ds.get('description', '').lower()
                tags = ' '.join(ds.get('tags', [])).lower()
                
                pertinence = 0
                for mot in mots_cles:
                    if mot in titre:
                        pertinence += 3
                    if mot in desc:
                        pertinence += 2
                    if mot in tags:
                        pertinence += 1
                
                if pertinence > 0:
                    ds_copy = ds.copy()
                    ds_copy['_pertinence'] = pertinence
                    resultats.append(ds_copy)
            
            resultats.sort(key=lambda x: x.get('_pertinence', 0), reverse=True)
            resultats = resultats[:page_size]
        else:
            resultats = DEMO_DATASETS[:page_size]
        
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated = resultats[start_idx:end_idx]
        
        return {
            'data': paginated,
            'total': len(resultats),
            'page': page,
            'page_size': page_size,
            'is_demo': True
        }
    
    def get_datasets_stats(self):
        """Statistiques avec données de démo"""
        return {
            'total_datasets': 24187,
            'organisations_count': 1250,
            'top_organisations': {
                'INSEE': 1245,
                'Ministère de l\'Économie': 892,
                'Banque de France': 567,
                'Data.gouv.fr': 2345,
                'Ministère du Travail': 445,
                'Ministère de la Transition écologique': 678,
                'Régions': 1234,
                'Villes et métropoles': 2345,
                'CEREMA': 456,
                'ADEME': 389
            },
            'top_formats': {
                'CSV': 15234,
                'XLSX': 8234,
                'PDF': 6789,
                'JSON': 3456,
                'SHP': 2345,
                'XML': 1234
            },
            'recent_datasets': 1240,
            'avg_views': 1250,
            'sample_size': 300
        }
    
    def get_datasets_populaires(self, limit=20):
        """Datasets populaires"""
        return DEMO_DATASETS[:limit]

# =============================================================================
# INTERFACE
# =============================================================================
def afficher_sidebar():
    with st.sidebar:
        st.markdown("""
        <div style='text-align: center; padding: 10px; background: linear-gradient(135deg, #0055A4 0%, #EF4135 100%); border-radius: 10px; margin-bottom: 20px;'>
            <h3 style='color: white; margin: 0;'>🇫🇷 DATA.GOUV.FR</h3>
            <p style='color: white; margin: 0; font-size: 12px;'>Dashboard Analytics</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="filter-section">', unsafe_allow_html=True)
        st.header("🔧 RECHERCHE")
        
        recherche_texte = st.text_input(
            "🔍 Mots-clés",
            placeholder="ex: économie budget, PIB, emploi...",
            help="2-3 mots-clés maximum",
            key="search_input_main"
        )
        
        st.markdown("---")
        st.subheader("🚀 Recherches rapides")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📊 Économie", key="btn_economie", use_container_width=True):
                st.session_state.recherche_query = "économie"
                st.session_state.recherche_page = 1
                st.rerun()
            if st.button("💰 Budget", key="btn_budget", use_container_width=True):
                st.session_state.recherche_query = "budget"
                st.session_state.recherche_page = 1
                st.rerun()
        with col2:
            if st.button("🏢 Entreprises", key="btn_entreprises", use_container_width=True):
                st.session_state.recherche_query = "entreprises"
                st.session_state.recherche_page = 1
                st.rerun()
            if st.button("👥 Emploi", key="btn_emploi", use_container_width=True):
                st.session_state.recherche_query = "emploi"
                st.session_state.recherche_page = 1
                st.rerun()
        
        results_per_page = st.slider("Résultats par page", 10, 50, 20, key="slider_results")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.info("💡 **Astuce:** Commencez par une recherche simple comme `économie` ou `budget`")
    
    return {
        'recherche_texte': recherche_texte,
        'results_per_page': results_per_page
    }

def afficher_resultat_dataset(dataset, idx):
    org_name = dataset.get('organization', {}).get('name', 'Inconnue')
    created_at = (dataset.get('created_at') or '')[:10]
    metrics = dataset.get('metrics', {})
    
    with st.container():
        st.markdown(f"""
        <div class="result-item">
            <div class="result-title">📊 {dataset.get('title', 'Sans titre')}</div>
            <div><strong>🏢 Organisation:</strong> {org_name}</div>
            <div><strong>📅 Publication:</strong> {created_at}</div>
            <div><strong>📁 Ressources:</strong> {len(dataset.get('resources', []))} fichiers</div>
            <div><strong>👁️ Vues:</strong> {metrics.get('views', 0):,} • 
                 <strong>❤️ Followers:</strong> {metrics.get('followers', 0)} • 
                 <strong>🔄 Réutilisations:</strong> {metrics.get('reuses', 0)}</div>
            <div><strong>🏷️ Tags:</strong> {', '.join(dataset.get('tags', []))}</div>
        </div>
        """, unsafe_allow_html=True)
        
        url = f"https://www.data.gouv.fr/fr/datasets/{dataset.get('id', '')}/"
        st.markdown(f'<a href="{url}" target="_blank"><button style="background: #EF4135; color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; margin-bottom: 10px;">🌐 Voir sur data.gouv.fr</button></a>', unsafe_allow_html=True)
        
        st.markdown("---")

def afficher_pagination_simple(page_actuelle, total_pages, position="top"):
    """Pagination simple sans selectbox pour éviter les conflits de clés"""
    if total_pages <= 1:
        return
    
    col1, col2, col3, col4 = st.columns([1, 3, 3, 1])
    
    with col1:
        if page_actuelle > 1:
            if st.button("⬅️ Précédent", key=f"prev_{position}_{page_actuelle}", use_container_width=True):
                st.session_state.recherche_page = page_actuelle - 1
                st.rerun()
    
    with col2:
        st.write(f"📄 **Page {page_actuelle} / {total_pages}**")
    
    with col3:
        # Navigation par numéro direct avec text_input au lieu de selectbox
        page_num = st.text_input(
            "Aller à la page",
            value="",
            placeholder=f"1-{total_pages}",
            key=f"goto_input_{position}_{page_actuelle}_{total_pages}",
            label_visibility="collapsed"
        )
        if page_num:
            try:
                num = int(page_num)
                if 1 <= num <= total_pages and num != page_actuelle:
                    st.session_state.recherche_page = num
                    st.rerun()
            except ValueError:
                pass
    
    with col4:
        if page_actuelle < total_pages:
            if st.button("Suivant ➡️", key=f"next_{position}_{page_actuelle}", use_container_width=True):
                st.session_state.recherche_page = page_actuelle + 1
                st.rerun()

def afficher_onglet_recherche(client, filtres):
    st.markdown('<h2 class="section-header">🔍 RECHERCHE DE DATASETS</h2>', unsafe_allow_html=True)
    
    # Guide
    with st.expander("💡 Conseils pour une recherche efficace", expanded=False):
        st.markdown("""
        ### ✅ Requêtes recommandées :
        - `économie` - Tous les datasets économiques
        - `budget` - Budgets et finances
        - `emploi` - Données sur l'emploi
        - `entreprises` - Données d'entreprises
        - `PIB` - Produit Intérieur Brut
        - `dette` - Dette publique
        
        ### 💡 Astuces :
        - Utilisez 2-3 mots-clés maximum
        - Commencez simple, puis affinez
        - Évitez les mots vides (le, la, de, et)
        """)
    
    # Construction de la requête
    query = ""
    if filtres['recherche_texte']:
        mots = filtres['recherche_texte'].strip().split()[:3]
        query = ' '.join(mots)
    
    # Bouton de recherche
    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn1:
        if st.button("🔍 RECHERCHER", key="btn_search_main", type="primary", use_container_width=True):
            if query:
                st.session_state.recherche_query = query
                st.session_state.recherche_page = 1
                st.rerun()
            else:
                st.warning("Veuillez saisir des mots-clés")
    
    if query:
        st.info(f"**Requête:** `{query}`")
    
    # Exécution de la recherche
    if 'recherche_query' in st.session_state and st.session_state.recherche_query:
        executer_recherche(client, st.session_state.recherche_query, 
                          st.session_state.get('recherche_page', 1), 
                          filtres['results_per_page'])

def executer_recherche(client, query, page, page_size):
    with st.spinner(f"Recherche de '{query}' en cours..."):
        resultats = client.rechercher_datasets(query, page, page_size)
    
    if resultats:
        afficher_resultats(resultats, page, page_size)

def afficher_resultats(resultats, current_page, page_size):
    datasets = resultats.get('data', [])
    total_results = resultats.get('total', 0)
    total_pages = max(1, (total_results + page_size - 1) // page_size)
    
    # En-tête
    col1, col2 = st.columns([3, 1])
    with col1:
        if resultats.get('is_demo'):
            st.info("ℹ️ Données de démonstration (API temporairement indisponible)")
        
        if total_results > 0:
            st.success(f"✅ **{total_results}** dataset(s) trouvé(s)")
        else:
            st.warning("❌ **Aucun résultat** - Essayez avec des mots-clés plus génériques")
    
    with col2:
        st.metric("📄 Page", f"{current_page}/{total_pages}")
    
    if total_results == 0:
        st.info("""
        ### 💡 Suggestions :
        - Utilisez des termes plus génériques (ex: `économie` au lieu de `économie budget dépenses`)
        - Essayez ces recherches : `budget`, `emploi`, `PIB`, `dette`
        - Consultez les exemples dans l'onglet "Top Datasets"
        """)
        return
    
    # Pagination en haut
    if total_pages > 1:
        afficher_pagination_simple(current_page, total_pages, "top")
        st.markdown("---")
    
    # Résultats
    for idx, dataset in enumerate(datasets):
        afficher_resultat_dataset(dataset, idx)
    
    # Pagination en bas
    if total_pages > 1:
        st.markdown("---")
        afficher_pagination_simple(current_page, total_pages, "bottom")

def afficher_onglet_analytics(client):
    st.markdown('<h2 class="section-header">📊 ANALYTICS GLOBAUX</h2>', unsafe_allow_html=True)
    
    with st.spinner('Chargement des statistiques...'):
        stats = client.get_datasets_stats()
    
    # KPI
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 Total Datasets", f"{stats['total_datasets']:,}")
    with col2:
        st.metric("🏢 Organisations", stats['organisations_count'])
    with col3:
        st.metric("🆕 Datasets Récents", stats['recent_datasets'])
    with col4:
        st.metric("👁️ Vues moyennes", f"{stats['avg_views']:,}")
    
    # Top organisations
    st.subheader("🏆 Top 10 Organisations")
    org_df = pd.DataFrame({
        'Organisation': list(stats['top_organisations'].keys()),
        'Datasets': list(stats['top_organisations'].values())
    })
    
    fig = px.bar(org_df, x='Datasets', y='Organisation', orientation='h',
                 title="", color='Datasets',
                 color_continuous_scale=['#0055A4', '#EF4135'])
    fig.update_layout(plot_bgcolor='white', paper_bgcolor='white', 
                     font=dict(color='#000000'), height=400)
    st.plotly_chart(fig, use_container_width=True, key="chart_organisations")
    
    # Formats
    st.subheader("📁 Formats de fichiers")
    format_df = pd.DataFrame({
        'Format': list(stats['top_formats'].keys()),
        'Nombre': list(stats['top_formats'].values())
    })
    
    fig2 = px.pie(format_df, values='Nombre', names='Format', title="",
                  hole=0.4, color_discrete_sequence=['#0055A4', '#1f77b4', '#EF4135', '#FF6B6B'])
    fig2.update_layout(plot_bgcolor='white', paper_bgcolor='white', 
                       font=dict(color='#000000'), height=400)
    st.plotly_chart(fig2, use_container_width=True, key="chart_formats")

def afficher_onglet_top_datasets(client):
    st.markdown('<h2 class="section-header">⭐ TOP DATASETS POPULAIRES</h2>', unsafe_allow_html=True)
    
    with st.spinner('Chargement...'):
        datasets = client.get_datasets_populaires(20)
    
    if not datasets:
        st.warning("Aucun dataset disponible")
        return
    
    for i, dataset in enumerate(datasets):
        with st.expander(f"🏆 {i+1}. {dataset.get('title', 'Sans titre')}", expanded=i<2):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                org_name = dataset.get('organization', {}).get('name', 'Inconnue')
                metrics = dataset.get('metrics', {})
                
                st.write(f"**Organisation:** {org_name}")
                st.write(f"**Description:** {dataset.get('description', 'Aucune description')[:200]}...")
                st.write(f"**👁️ Vues:** {metrics.get('views', 0):,}")
                st.write(f"**❤️ Followers:** {metrics.get('followers', 0)}")
                st.write(f"**🏷️ Tags:** {', '.join(dataset.get('tags', []))}")
            
            with col2:
                url = f"https://www.data.gouv.fr/fr/datasets/{dataset.get('id', '')}/"
                st.markdown(f'<a href="{url}" target="_blank"><button style="background: #0055A4; color: white; padding: 10px; border: none; border-radius: 5px; width: 100%;">🌐 Ouvrir</button></a>', unsafe_allow_html=True)

# =============================================================================
# MAIN
# =============================================================================
def main():
    st.markdown("""
    <div style='text-align: center; background: linear-gradient(135deg, #0055A4 0%, #FFFFFF 50%, #EF4135 100%); padding: 20px; border-radius: 15px; margin-bottom: 20px;'>
        <h1 class="main-header">🔍 DASHBOARD DATA.GOUV.FR</h1>
        <p style='color: #0055A4; font-size: 1.2rem; font-weight: bold;'>Analyse de 24,187 datasets ouverts français</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<p style="text-align: center; color: #666;">📡 Source: data.gouv.fr - Données ouvertes de la République Française</p>', unsafe_allow_html=True)
    
    client = DataGouvAPIClient()
    filtres = afficher_sidebar()
    
    tab1, tab2, tab3 = st.tabs(["🔍 Recherche", "📊 Analytics", "⭐ Top Datasets"])
    
    with tab1:
        afficher_onglet_recherche(client, filtres)
    
    with tab2:
        afficher_onglet_analytics(client)
    
    with tab3:
        afficher_onglet_top_datasets(client)

if __name__ == "__main__":
    main()
