"""Scraper de produits Revolution Beauty.

Ce script extrait les informations détaillées de chaque page produit :
- Nom, description, prix
- Images (toutes les variantes)
- Breadcrumbs (fil d'Ariane)
- Notes et avis
- Ingrédients
- Variantes de produits (couleurs, tailles, etc.)

Le scraping est multi-threadé pour des performances optimales et les données
sont sauvegardées progressivement dans un fichier CSV.

Usage:
    python scrapper.py

Entrée:
    data/urls.csv - Fichier contenant les URLs de produits (généré par crawler.py)

Sortie:
    data/produits.csv - Fichier CSV contenant toutes les informations produits
"""

import csv
import requests
import time
import threading
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# ================= CONFIG =================
# Fichier CSV contenant les URLs de produits à scraper (généré par crawler.py)
INPUT_URLS = "data/urls.csv"
# Fichier CSV de sortie contenant les données extraites
OUTPUT_PRODUCTS = "data/produits.csv"

# Nombre de threads simultanés pour le scraping parallèle
MAX_WORKERS = 12
# Timeout en secondes pour les requêtes HTTP
TIMEOUT = 15
# Délai en secondes entre chaque requête (pour éviter de surcharger le serveur)
DELAY = 0.2

# En-têtes HTTP pour simuler un navigateur web
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

# Lock pour synchroniser l'écriture dans le fichier CSV entre threads
lock = threading.Lock()

# ================= HELPERS =================
def fetch(url):
    """Récupère le contenu HTML d'une URL.
    
    Ajoute un délai avant la requête pour respecter le serveur.
    
    Args:
        url (str): URL de la page à récupérer
        
    Returns:
        str|None: Contenu HTML de la page ou None en cas d'erreur
    """
    time.sleep(DELAY)
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        print(f"❌ Erreur fetch URL {url}: {e}")
    return None


def is_product_page(soup):
    """Vérifie si la page HTML correspond à une page produit.
    
    Args:
        soup (BeautifulSoup): Objet BeautifulSoup de la page
        
    Returns:
        bool: True si c'est une page produit
    """
    return soup.select_one("div.l-pdp-content_inner") is not None


def text(soup, selector):
    """Extrait le texte d'un élément HTML via un sélecteur CSS.
    
    Args:
        soup (BeautifulSoup): Objet BeautifulSoup de la page
        selector (str): Sélecteur CSS de l'élément
        
    Returns:
        str: Texte de l'élément (nettoyé) ou chaîne vide si non trouvé
    """
    el = soup.select_one(selector)
    return el.get_text(strip=True) if el else ""


def attr(soup, selector, attr):
    """Extrait la valeur d'un attribut d'un élément HTML.
    
    Args:
        soup (BeautifulSoup): Objet BeautifulSoup de la page
        selector (str): Sélecteur CSS de l'élément
        attr (str): Nom de l'attribut à extraire
        
    Returns:
        str: Valeur de l'attribut ou chaîne vide si non trouvé
    """
    el = soup.select_one(selector)
    return el.get(attr) if el else ""


# ================= EXTRACT =================
def extract_product(url, soup):
    """Extrait toutes les informations d'un produit depuis la page HTML.
    
    Parse le HTML de la page produit et extrait :
    - Informations de base (nom, description, prix)
    - Images (toutes les variantes disponibles)
    - Fil d'Ariane (breadcrumbs) pour la catégorisation
    - Note et nombre d'avis clients
    - Ingrédients du produit
    - Variantes disponibles (couleurs, tailles, etc.)
    
    Args:
        url (str): URL de la page produit
        soup (BeautifulSoup): Objet BeautifulSoup de la page HTML
        
    Returns:
        dict: Dictionnaire contenant toutes les informations du produit
    """
    # --- Extraction des images ---
    # Récupère toutes les images du produit (galerie complète)
    images = []
    for img in soup.select(".l-pdp-product_images img"):
        src = img.get("src")
        if src and src not in images:  # Évite les doublons
            images.append(src)

    # --- Extraction du fil d'Ariane (breadcrumbs) ---
    # Crée une chaîne "Catégorie > Sous-catégorie > Produit"
    breadcrumbs = " > ".join(
        a.get_text(strip=True)
        for a in soup.select("ul.b-breadcrumbs a")
    )

    # --- Extraction des ingrédients ---
    ingredients = text(soup, ".b-ingredients")

    # --- Extraction des variantes produit ---
    # Récupère toutes les variantes (couleurs, tailles, etc.)
    variantes = []
    for var in soup.select(".b-swatch_colors-item"):
        variantes.append({
            "name": var.get("data-js-display-value", ""),
            "id": var.get("data-js-variant-id", ""),
            "url": var.get("data-js-url", "")
        })
    # Convertit la liste en JSON pour stockage dans le CSV
    variantes_str = json.dumps(variantes, ensure_ascii=False)

    # --- Construction du dictionnaire produit ---
    return {
        "url": url,
        "name": text(soup, "h1.b-product_name"),
        "description": text(soup, "p.b-product_summary"),
        "price_sale": text(soup, "span.b-product_price-sales span.b-product_price-value"),
        "price_original": text(soup, "span.b-product_price-list span.b-product_price-value"),
        "discount": text(soup, "div.b-product_price-discount"),
        "breadcrumbs": breadcrumbs,
        "rating": attr(soup, ".yotpo-stars", "data-product-rating"),
        "reviews": text(soup, ".yotpo-bottomline a"),
        "images": "|".join(images),  # Images séparées par |
        "ingredients": ingredients,
        "variantes": variantes_str,  # JSON stringifié
    }


# ================= WORKER =================
def process_url(url, csv_file, fieldnames):
    """Traite une URL : récupère, extrait et sauvegarde les données produit.
    
    Fonction exécutée par chaque thread worker :
    1. Récupère le HTML de la page
    2. Vérifie que c'est bien une page produit
    3. Extrait toutes les informations
    4. Sauvegarde immédiatement dans le CSV (thread-safe)
    
    Args:
        url (str): URL de la page produit à scraper
        csv_file (str): Chemin du fichier CSV de sortie
        fieldnames (list): Liste des noms de colonnes du CSV
    """
    # Récupère le HTML de la page
    html = fetch(url)
    if not html:
        return

    # Parse le HTML avec BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Vérifie que c'est bien une page produit
    if not is_product_page(soup):
        return

    # Extrait toutes les informations du produit
    product = extract_product(url, soup)

    # Sauvegarde immédiate dans le CSV (thread-safe)
    with lock:
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(product)
        print("✅", product["name"])


# ================= MAIN =================
def main():
    """Fonction principale de scraping.
    
    Orchestre le processus de scraping multi-threadé :
    1. Charge la liste des URLs depuis le fichier CSV
    2. Initialise le fichier CSV de sortie avec les en-têtes
    3. Lance le scraping parallèle de toutes les URLs
    4. Sauvegarde progressive des résultats (au fur et à mesure)
    """
    # --- Chargement des URLs à scraper ---
    with open(INPUT_URLS, newline="", encoding="utf-8") as f:
        urls = [row["url"] for row in csv.DictReader(f)]

    # --- Définition des colonnes du CSV de sortie ---
    fieldnames = [
        "url",           # URL de la page produit
        "name",          # Nom du produit
        "description",   # Description du produit
        "price_sale",    # Prix de vente actuel
        "price_original",# Prix original (avant réduction)
        "discount",      # Réduction (pourcentage ou montant)
        "breadcrumbs",   # Fil d'Ariane (catégorisation)
        "rating",        # Note moyenne du produit
        "reviews",       # Nombre d'avis clients
        "images",        # URLs des images (séparées par |)
        "ingredients",   # Liste des ingrédients
        "variantes"      # Variantes disponibles (JSON)
    ]

    # --- Création du fichier CSV avec en-têtes ---
    with open(OUTPUT_PRODUCTS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

    # --- Lancement du scraping multi-threadé ---
    # Chaque URL est traitée en parallèle par un thread
    # Les résultats sont sauvegardés au fur et à mesure
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_url, url, OUTPUT_PRODUCTS, fieldnames) for url in urls]
        # Attend que tous les threads terminent
        for _ in as_completed(futures):
            pass

    print("\n🎉 SCRAPING MULTITHREAD TERMINÉ")


if __name__ == "__main__":
    main()
