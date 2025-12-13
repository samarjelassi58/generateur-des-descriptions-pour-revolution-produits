"""
Script de nettoyage des données de produits Revolution Beauty

Ce script effectue un nettoyage complet des données de produits cosmétiques :
- Suppression des doublons
- Normalisation des prix (conversion en float)
- Nettoyage des réductions et avis (extraction des nombres)
- Conversion des images en format liste JSON
- Normalisation du texte (minuscules + suppression des accents)
- Extraction et normalisation des breadcrumbs en colonnes de catégories

Input: data/produits_local.csv
Output: data/cleaned_products.csv

Auteur: Samar Jelassi
Date: Décembre 2025
"""

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
    
    Args:
        text (str): Texte avec accents
        
    Returns:
        str: Texte sans accents
        
    Exemples:
        >>> remove_accents("été")
        'ete'
        >>> remove_accents("crème")
        'creme'
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
    Nettoie et convertit les prix en format numérique.
    
    Args:
        price (str): Prix au format européen (€12,99)
        
    Returns:
        float: Prix en format décimal (12.99) ou None si invalide
        
    Exemples:
        >>> clean_price("€12,99")
        12.99
        >>> clean_price("6,99")
        6.99
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
    Extrait la valeur numérique des réductions.
    
    Args:
        discount (str): Réduction au format texte (ex: "(-50%)")
        
    Returns:
        int: Valeur de la réduction ou None si invalide
        
    Exemples:
        >>> clean_discount("(-50%)")
        50
        >>> clean_discount("-25%")
        25
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
    Extrait le nombre d'avis depuis une chaîne de texte.
    
    Args:
        reviews (str): Nombre d'avis au format texte (ex: "5 Reviews")
        
    Returns:
        int: Nombre d'avis ou None si invalide
        
    Exemples:
        >>> clean_reviews("5 Reviews")
        5
        >>> clean_reviews("123 Reviews")
        123
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
    Convertit une chaîne d'images séparées par | en liste JSON.
    
    Args:
        images (str): Chemins d'images séparés par |
        
    Returns:
        str: Liste JSON des chemins d'images
        
    Exemples:
        >>> split_images("img1.jpg|img2.jpg")
        '["img1.jpg", "img2.jpg"]'
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
    Parse et normalise les variantes de produit en JSON.
    
    Args:
        variantes (str/list): Variantes au format JSON string ou liste Python
        
    Returns:
        str: Variantes en format JSON structuré
        
    Exemples:
        >>> parse_variantes('[{"name": "Light", "id": "123"}]')
        '[{"name": "Light", "id": "123"}]'
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
    Normalise le texte : minuscules + suppression des accents.
    
    Args:
        text (str): Texte à nettoyer
        
    Returns:
        str: Texte normalisé
        
    Exemples:
        >>> clean_text("Revolution Beauté")
        'revolution beaute'
        >>> clean_text("Crème Hydratante")
        'creme hydratante'
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
    Extrait les breadcrumbs depuis l'URL pour les produits sans breadcrumbs.
    
    Args:
        url (str): URL du produit
        
    Returns:
        str: Breadcrumbs au format "Category1 > Category2 > Category3" ou None
        
    Exemples:
        >>> extract_breadcrumbs_from_url(".../maquillage/teint/blush/...")
        'Maquillage > Teint > Blush'
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

# =============================================================================
# SCRIPT PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("NETTOYAGE DES DONNEES DE PRODUITS REVOLUTION BEAUTY")
    print("=" * 70)
    
    # Lire le CSV
    print(f"\n1. Lecture du fichier: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV)
    print(f"   Lignes initiales: {len(df)}")
    
    # Supprimer les doublons basés sur le nom
    print("\n2. Suppression des doublons...")
    df = df.drop_duplicates(subset=['name'], keep='first')
    print(f"   Lignes après dédoublonnage: {len(df)}")
    
    # Supprimer les colonnes inutiles
    print("\n3. Suppression des colonnes inutiles...")
    columns_to_drop = []
    if 'ingredients' in df.columns:
        columns_to_drop.append('ingredients')
    if 'description' in df.columns:
        columns_to_drop.append('description')
    
    if columns_to_drop:
        df = df.drop(columns=columns_to_drop)
        print(f"   Colonnes supprimées: {', '.join(columns_to_drop)}")
    
    # Nettoyer les colonnes textuelles
    print("\n4. Nettoyage du texte (minuscules + suppression accents)...")
    text_columns = ['name', 'breadcrumbs', 'url']
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)
            print(f"   ✓ {col}")
    
    # Remplir les breadcrumbs manquants
    print("\n5. Extraction des breadcrumbs depuis les URLs...")
    if 'breadcrumbs' in df.columns and 'url' in df.columns:
        missing_count = df['breadcrumbs'].isna().sum()
        if missing_count > 0:
            missing_breadcrumbs = df['breadcrumbs'].isna()
            df.loc[missing_breadcrumbs, 'breadcrumbs'] = df.loc[missing_breadcrumbs, 'url'].apply(extract_breadcrumbs_from_url)
            df['breadcrumbs'] = df['breadcrumbs'].apply(clean_text)
            print(f"   Breadcrumbs extraits: {missing_count}")
        else:
            print(f"   Aucun breadcrumb manquant")
    
    # Nettoyer les prix
    print("\n6. Nettoyage des prix...")
    if 'price_sale' in df.columns:
        df['price_sale'] = df['price_sale'].apply(clean_price)
        print(f"   ✓ price_sale (€12,99 → 12.99)")
    
    if 'price_original' in df.columns:
        df['price_original'] = df['price_original'].apply(clean_price)
        print(f"   ✓ price_original")
    
    # Nettoyer les réductions
    print("\n7. Nettoyage des réductions...")
    if 'discount' in df.columns:
        df['discount'] = df['discount'].apply(clean_discount)
        discount_count = df['discount'].notna().sum()
        print(f"   ✓ discount ((-50%) → 50) - {discount_count} produits en promotion")
    
    # Nettoyer les avis
    print("\n8. Nettoyage des avis...")
    if 'reviews' in df.columns:
        df['reviews'] = df['reviews'].apply(clean_reviews)
        print(f"   ✓ reviews (5 Reviews → 5)")
    
    # Nettoyer les images
    print("\n9. Conversion des images en liste JSON...")
    if 'images' in df.columns:
        df['images'] = df['images'].apply(split_images)
        print(f"   ✓ images (split par | → JSON array)")
    
    # Parser les variantes
    print("\n10. Parsing des variantes...")
    if 'variantes' in df.columns:
        df['variantes'] = df['variantes'].apply(parse_variantes)
        variants_count = df[df['variantes'] != '[]'].shape[0]
        print(f"   ✓ variantes parsées - {variants_count} produits avec variantes")
    
    # Normaliser les breadcrumbs en colonnes
    print("\n11. Normalisation des breadcrumbs en colonnes...")
    if 'breadcrumbs' in df.columns:
        breadcrumb_split = df['breadcrumbs'].str.split('>', expand=True)
        
        for col in breadcrumb_split.columns:
            breadcrumb_split[col] = breadcrumb_split[col].str.strip()
        
        num_levels = breadcrumb_split.shape[1]
        
        if num_levels >= 1:
            df['category_1'] = breadcrumb_split[0]
            print(f"   ✓ category_1 créée")
        if num_levels >= 2:
            df['category_2'] = breadcrumb_split[1]
            print(f"   ✓ category_2 créée")
        if num_levels >= 3:
            df['category_3'] = breadcrumb_split[2]
            print(f"   ✓ category_3 créée")
        if num_levels >= 4:
            df['category_4'] = breadcrumb_split[3]
            print(f"   ✓ category_4 créée")
    
    # Sauvegarder le résultat
    print(f"\n12. Sauvegarde du fichier nettoyé...")
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"   ✓ Fichier sauvegardé: {OUTPUT_CSV}")
    
    # Résumé final
    print("\n" + "=" * 70)
    print("RÉSUMÉ")
    print("=" * 70)
    print(f"Nombre total de produits: {len(df)}")
    print(f"Nombre de colonnes: {len(df.columns)}")
    print(f"\nColonnes finales:")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i:2d}. {col}")
    print("\n✓ Nettoyage terminé avec succès!")
    print("=" * 70)
