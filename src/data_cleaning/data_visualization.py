import pandas as pd
import plotly.express as px
import plotly.io as pio

# ---------------- CHARGEMENT ---------------- #
df = pd.read_csv("data/produits.csv")

# ---------------- NETTOYAGE RAPIDE DES COLONNES NUMÉRIQUES ---------------- #
def parse_price(x):
    try:
        return float(str(x).replace('€','').replace(',','.'))
    except:
        return None

df['price_sale'] = df['price_sale'].apply(parse_price)
df['price_original'] = df['price_original'].apply(parse_price)
df['rating'] = pd.to_numeric(df['rating'], errors='coerce')

# Extraire les 3 niveaux de catégories depuis breadcrumbs
def extract_categories(breadcrumb):
    if pd.isna(breadcrumb):
        return 'Unknown', 'Unknown', 'Unknown'
    
    parts = [p.strip() for p in str(breadcrumb).split('>')]
    cat1 = parts[0] if len(parts) > 0 else 'Unknown'
    cat2 = parts[1] if len(parts) > 1 else 'Unknown'
    cat3 = parts[2] if len(parts) > 2 else 'Unknown'
    
    return cat1, cat2, cat3

df[['categorie', 'sous_categorie', 'sous_sous_categorie']] = df['breadcrumbs'].apply(
    lambda x: pd.Series(extract_categories(x))
)

# ---------------- CONFIGURATION THÈME ---------------- #
theme_colors = {
    'primary': '#6366f1',
    'secondary': '#ec4899',
    'success': '#10b981',
    'warning': '#f59e0b',
    'danger': '#ef4444',
    'info': '#06b6d4'
}

# ---------------- GRAPHIQUES UTILES POUR NETTOYAGE ---------------- #
figs = []

# 1️⃣ Catégories principales (Pie chart avec pourcentages)
cat_counts = df['categorie'].value_counts().reset_index()
cat_counts.columns = ['categorie', 'count']
cat_counts['percentage'] = (cat_counts['count'] / cat_counts['count'].sum() * 100).round(1)

fig_categorie = px.pie(
    cat_counts,
    values='count',
    names='categorie',
    title="📊 Répartition des catégories principales (%)",
    hole=0.4,
    color_discrete_sequence=px.colors.qualitative.Set3
)
fig_categorie.update_traces(
    textposition='inside',
    textinfo='percent+label',
    hovertemplate='<b>%{label}</b><br>Nombre: %{value}<br>Pourcentage: %{percent}<extra></extra>'
)
fig_categorie.update_layout(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font=dict(family="Segoe UI, sans-serif", size=12),
    title_font=dict(size=20, color='#1f2937')
)
figs.append(fig_categorie)

# 1.2️⃣ Sous-catégories par catégorie principale
import plotly.graph_objects as go

for categorie in df['categorie'].unique():
    if categorie != 'Unknown':
        sous_cat = df[df['categorie'] == categorie]['sous_categorie'].value_counts().reset_index()
        sous_cat.columns = ['sous_categorie', 'count']
        sous_cat['percentage'] = (sous_cat['count'] / sous_cat['count'].sum() * 100).round(1)
        
        fig_sous_cat = px.bar(
            sous_cat,
            x='sous_categorie',
            y='count',
            title=f"📂 Sous-catégories de '{categorie}' (Total: {sous_cat['count'].sum()})",
            labels={'sous_categorie': 'Sous-catégorie', 'count': 'Nombre de produits'},
            color='percentage',
            color_continuous_scale='Viridis',
            text='percentage'
        )
        fig_sous_cat.update_traces(
            texttemplate='%{text:.1f}%',
            textposition='outside'
        )
        fig_sous_cat.update_layout(
            xaxis_tickangle=-45,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(family="Segoe UI, sans-serif", size=12),
            title_font=dict(size=18, color='#1f2937'),
            showlegend=False
        )
        figs.append(fig_sous_cat)

# 1.3️⃣ Sunburst : Vue hiérarchique complète
fig_sunburst = px.sunburst(
    df[df['categorie'] != 'Unknown'],
    path=['categorie', 'sous_categorie', 'sous_sous_categorie'],
    title="🌞 Vue hiérarchique des catégories (Sunburst)",
    color_discrete_sequence=px.colors.qualitative.Pastel
)
fig_sunburst.update_layout(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    title_font=dict(size=20, color='#1f2937'),
    height=700
)
figs.append(fig_sunburst)

# 2️⃣ Valeurs manquantes par colonne
missing = df.isna().sum().reset_index()
missing.columns = ['Colonne', 'Valeurs manquantes']

fig_missing = px.bar(
    missing,
    x='Colonne',
    y='Valeurs manquantes',
    title="⚠️ Valeurs manquantes par colonne",
    color_discrete_sequence=[theme_colors['warning']]
)
fig_missing.update_layout(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font=dict(family="Segoe UI, sans-serif", size=12),
    title_font=dict(size=20, color='#1f2937')
)
figs.append(fig_missing)

# 3️⃣ Produits avec/sans images
df['has_images'] = df['images'].notna() & (df['images'].str.len() > 0)
image_counts = df['has_images'].value_counts().reset_index()
image_counts.columns = ['has_images', 'count']
image_counts['has_images'] = image_counts['has_images'].map({True: 'Avec images', False: 'Sans images'})

fig_images = px.pie(
    image_counts,
    values='count',
    names='has_images',
    title="🖼️ Répartition des produits avec/sans images",
    color_discrete_sequence=[theme_colors['success'], theme_colors['danger']],
    hole=0.4
)
fig_images.update_layout(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font=dict(family="Segoe UI, sans-serif", size=12),
    title_font=dict(size=20, color='#1f2937')
)
figs.append(fig_images)

# 5️⃣ Détection de produits dupliqués
duplicates_name = df[df.duplicated(subset=['name'], keep=False)]
duplicates_url = df[df.duplicated(subset=['url'], keep=False)]

duplicate_stats = pd.DataFrame({
    'Type': ['Noms dupliqués', 'URLs dupliquées', 'Produits uniques'],
    'Nombre': [
        len(duplicates_name),
        len(duplicates_url),
        len(df) - len(duplicates_name)
    ]
})

fig_duplicates = px.bar(
    duplicate_stats,
    x='Type',
    y='Nombre',
    title="🔍 Détection de produits dupliqués",
    color='Type',
    color_discrete_sequence=[theme_colors['danger'], theme_colors['warning'], theme_colors['success']]
)
fig_duplicates.update_layout(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font=dict(family="Segoe UI, sans-serif", size=12),
    title_font=dict(size=20, color='#1f2937'),
    showlegend=False
)
figs.append(fig_duplicates)

# 6️⃣ Distribution des prix
df_with_price = df[df['price_sale'].notna()]
fig_price_dist = px.histogram(
    df_with_price,
    x='price_sale',
    nbins=30,
    title="💰 Distribution des prix de vente",
    labels={'price_sale': 'Prix (€)'},
    color_discrete_sequence=[theme_colors['success']]
)
fig_price_dist.update_layout(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font=dict(family="Segoe UI, sans-serif", size=12),
    title_font=dict(size=20, color='#1f2937')
)
figs.append(fig_price_dist)

# 7️⃣ Analyse des descriptions
df['has_description'] = df['description'].notna() & (df['description'].str.len() > 0)
desc_stats = df['has_description'].value_counts().reset_index()
desc_stats.columns = ['has_description', 'count']
desc_stats['has_description'] = desc_stats['has_description'].map({True: 'Avec description', False: 'Sans description'})

fig_desc = px.pie(
    desc_stats,
    values='count',
    names='has_description',
    title="📝 Produits avec/sans description",
    color_discrete_sequence=[theme_colors['info'], theme_colors['danger']],
    hole=0.4
)
fig_desc.update_layout(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font=dict(family="Segoe UI, sans-serif", size=12),
    title_font=dict(size=20, color='#1f2937')
)
figs.append(fig_desc)

# ---------------- EXPORT HTML AVEC DESIGN MODERNE ---------------- #
html_file = "nettoyage_produits.html"

html_header = """<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard Nettoyage Produits - Relovution</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 2rem;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        .header {{
            background: white;
            border-radius: 20px;
            padding: 2rem 3rem;
            margin-bottom: 2rem;
            box-shadow: 0 20px 60px rgba(0,0,0,0.15);
        }}
        .header h1 {{
            color: #1f2937;
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
        }}
        .header p {{
            color: #6b7280;
            font-size: 1.1rem;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}
        .stat-card {{
            background: white;
            border-radius: 15px;
            padding: 1.5rem;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
            border-left: 4px solid #6366f1;
        }}
        .stat-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 15px 40px rgba(0,0,0,0.2);
        }}
        .stat-card.danger {{
            border-left-color: #ef4444;
        }}
        .stat-card.warning {{
            border-left-color: #f59e0b;
        }}
        .stat-card.success {{
            border-left-color: #10b981;
        }}
        .stat-card h3 {{
            color: #6b7280;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 0.5rem;
        }}
        .stat-card p {{
            color: #1f2937;
            font-size: 2rem;
            font-weight: 700;
        }}
        .chart-container {{
            background: white;
            border-radius: 20px;
            padding: 2rem;
            margin-bottom: 2rem;
            box-shadow: 0 20px 60px rgba(0,0,0,0.15);
        }}
        .grid-2 {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 2rem;
        }}
        @media (max-width: 768px) {{
            .grid-2 {{
                grid-template-columns: 1fr;
            }}
            .header h1 {{
                font-size: 2rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎨 Dashboard Nettoyage Produits</h1>
            <p>Analyse complète des données produits - Relovution NLP</p>
        </div>
        
        <div class="stats">
            <div class="stat-card">
                <h3>Total Produits</h3>
                <p>{total_products}</p>
            </div>
            <div class="stat-card">
                <h3>Catégories</h3>
                <p>{total_categories}</p>
            </div>
            <div class="stat-card danger">
                <h3>Sans Description</h3>
                <p>{missing_desc}</p>
            </div>
            <div class="stat-card warning">
                <h3>Prix Manquants</h3>
                <p>{missing_price}</p>
            </div>
            <div class="stat-card success">
                <h3>Avec Images</h3>
                <p>{products_with_images}</p>
            </div>
            <div class="stat-card danger">
                <h3>Doublons</h3>
                <p>{duplicates}</p>
            </div>
        </div>
""".format(
    total_products=len(df),
    total_categories=df['categorie'].nunique(),
    missing_desc=df['description'].isna().sum(),
    missing_price=df['price_sale'].isna().sum(),
    products_with_images=len(df[df['has_images']]),
    duplicates=len(duplicates_name)
)

html_footer = """
    </div>
</body>
</html>
"""

with open(html_file, 'w', encoding='utf-8') as f:
    f.write(html_header)
    
    # Graphiques individuels
    for i, fig in enumerate(figs):
        f.write('<div class="chart-container">')
        f.write(pio.to_html(fig, full_html=False, include_plotlyjs='cdn' if i == 0 else False))
        f.write('</div>')
    
    f.write(html_footer)

print(f"✅ Fichier HTML de nettoyage généré : {html_file}")
