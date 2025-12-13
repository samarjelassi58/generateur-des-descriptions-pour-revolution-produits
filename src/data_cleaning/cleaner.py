import pandas as pd
import urllib.parse
import unicodedata
import re
import json

INPUT_CSV = "data/produits_local.csv"
OUTPUT_CSV = "data/cleaned_products.csv"

def remove_accents(text):
    """
    Supprime les accents d'une chaîne de caractères.
    é, è, ê → e
    à, â → a
    etc.
    """
    if pd.isna(text):
        return text
    
    # Normaliser en décomposant les caractères accentués
    nfd = unicodedata.normalize('NFD', str(text))
    # Supprimer les marques diacritiques
    without_accents = ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')
    return without_accents

def clean_price(price):
    """
    Nettoie les prix : €12,99 -> 12.99
    """
    if pd.isna(price) or price == '':
        return None
    
    price_str = str(price).strip()
    
    # Supprimer €, espaces et autres caractères non numériques sauf , et .
    price_str = re.sub(r'[^\d,.]', '', price_str)
    
    # Remplacer la virgule par un point (format européen -> format standard)
    price_str = price_str.replace(',', '.')
    
    # Convertir en float
    try:
        return float(price_str)
    except:
        return None

def clean_discount(discount):
    """
    Nettoie les réductions : (-50%) -> 50
    """
    if pd.isna(discount) or discount == '':
        return None
    
    discount_str = str(discount).strip()
    
    # Extraire uniquement les chiffres
    numbers = re.findall(r'\d+', discount_str)
    
    if numbers:
        return int(numbers[0])
    return None

def clean_reviews(reviews):
    """
    Nettoie les avis : 5 Reviews -> 5
    """
    if pd.isna(reviews) or reviews == '':
        return None
    
    reviews_str = str(reviews).strip()
    
    # Extraire uniquement les chiffres
    numbers = re.findall(r'\d+', reviews_str)
    
    if numbers:
        return int(numbers[0])
    return None

def split_images(images):
    """
    Sépare les images en liste : path1|path2|path3 -> ['path1', 'path2', 'path3']
    """
    if pd.isna(images) or images == '':
        return '[]'
    
    images_str = str(images).strip()
    
    # Séparer par le délimiteur |
    images_list = [img.strip() for img in images_str.split('|') if img.strip()]
    
    # Retourner en format JSON
    return json.dumps(images_list)

def parse_variantes(variantes):
    """
    Parse les variantes depuis JSON string vers format structuré
    """
    if pd.isna(variantes) or variantes == '' or variantes == '[]':
        return '[]'
    
    try:
        # Si c'est déjà une liste Python, la convertir en JSON
        if isinstance(variantes, list):
            return json.dumps(variantes)
        
        # Si c'est une string, essayer de la parser
        variantes_str = str(variantes).strip()
        
        # Parser le JSON
        variantes_data = json.loads(variantes_str)
        
        # Retourner en format JSON propre
        return json.dumps(variantes_data)
    except:
        return '[]'

def clean_text(text):
    """
    Nettoie le texte : convertit en minuscules et supprime les accents
    """
    if pd.isna(text) or text == '':
        return text
    
    text_str = str(text).strip()
    
    # Convertir en minuscules
    text_str = text_str.lower()
    
    # Supprimer les accents
    text_str = remove_accents(text_str)
    
    return text_str

def extract_breadcrumbs_from_url(url):
    """
    Extrait les breadcrumbs depuis l'URL.
    Exemple: /maquillage/lèvres/kits-lèvres/ -> Maquillage > Lèvres > Kits lèvres
    """
    try:
        # Parser l'URL
        parsed = urllib.parse.urlparse(url)
        path = parsed.path
        
        # Extraire les parties du chemin
        parts = path.split('/')
        
        # Filtrer les parties vides et les fichiers .html
        breadcrumb_parts = []
        for part in parts:
            if part and not part.endswith('.html') and part != 'intl' and part != 'fr':
                # Décoder les caractères URL encodés
                decoded = urllib.parse.unquote(part)
                # Remplacer les tirets par des espaces et capitaliser
                formatted = decoded.replace('-', ' ').title()
                breadcrumb_parts.append(formatted)
        
        # Joindre avec ' > ' en enlevant le dernier élément (nom du produit)
        if len(breadcrumb_parts) > 1:
            return ' > '.join(breadcrumb_parts[:-1])
        
    except Exception as e:
        print(f"Erreur lors de l'extraction des breadcrumbs: {e}")
    
    return None

# Lire le CSV
df = pd.read_csv(INPUT_CSV)

# Supprimer les doublons basés sur le nom
df = df.drop_duplicates(subset=['name'], keep='first')

# Supprimer la colonne 'ingredients' si elle existe
if 'ingredients' in df.columns:
    df = df.drop(columns=['ingredients'])

if 'description' in df.columns:
    df = df.drop(columns=['description'])

# Nettoyer les colonnes textuelles (minuscules + suppression accents)
text_columns = ['name', 'breadcrumbs', 'url']
for col in text_columns:
    if col in df.columns:
        df[col] = df[col].apply(clean_text)

# Remplir les breadcrumbs manquants à partir des URLs
if 'breadcrumbs' in df.columns and 'url' in df.columns:
    missing_breadcrumbs = df['breadcrumbs'].isna()
    df.loc[missing_breadcrumbs, 'breadcrumbs'] = df.loc[missing_breadcrumbs, 'url'].apply(extract_breadcrumbs_from_url)
    # Re-nettoyer les breadcrumbs extraits
    df['breadcrumbs'] = df['breadcrumbs'].apply(clean_text)

# Nettoyer les colonnes de prix
if 'price_sale' in df.columns:
    df['price_sale'] = df['price_sale'].apply(clean_price)

if 'price_original' in df.columns:
    df['price_original'] = df['price_original'].apply(clean_price)

# Nettoyer les réductions
if 'discount' in df.columns:
    df['discount'] = df['discount'].apply(clean_discount)

# Nettoyer les avis
if 'reviews' in df.columns:
    df['reviews'] = df['reviews'].apply(clean_reviews)

# Nettoyer les images (split en liste)
if 'images' in df.columns:
    df['images'] = df['images'].apply(split_images)

# Parser les variantes
if 'variantes' in df.columns:
    df['variantes'] = df['variantes'].apply(parse_variantes)

# Normaliser les breadcrumbs en colonnes séparées
if 'breadcrumbs' in df.columns:
    # Séparer les breadcrumbs en niveaux de catégories
    breadcrumb_split = df['breadcrumbs'].str.split('>', expand=True)
    
    # Nettoyer les espaces
    for col in breadcrumb_split.columns:
        breadcrumb_split[col] = breadcrumb_split[col].str.strip()
    
    # Renommer les colonnes selon le nombre de niveaux
    num_levels = breadcrumb_split.shape[1]
    
    if num_levels >= 1:
        df['category_1'] = breadcrumb_split[0]
    if num_levels >= 2:
        df['category_2'] = breadcrumb_split[1]
    if num_levels >= 3:
        df['category_3'] = breadcrumb_split[2]
    if num_levels >= 4:
        df['category_4'] = breadcrumb_split[3]

# Sauvegarder le résultat
df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
