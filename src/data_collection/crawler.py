"""
Crawler web pour extraire les URLs de produits du site Revolution Beauty.

Ce script parcourt les pages du site Revolution Beauty (maquillage, soin, cheveux, corps)
et extrait toutes les URLs de pages produits (se terminant par .html).

Fonctionnalités:
- Crawling multi-threadé pour des performances optimales
- Filtrage des URLs par domaine et chemin autorisé
- Détection automatique des pages produits
- Sauvegarde progressive dans un fichier CSV
- Respect d'un délai entre les requêtes pour éviter de surcharger le serveur

Usage:
    python crawler.py

Sortie:
    data/urls.csv - Fichier contenant toutes les URLs de produits trouvées
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

# ---------------- CONFIG ---------------- #
# URLs de départ pour le crawling (pages catégories principales)
START_URLS = [
    "https://www.revolutionbeauty.com/intl/fr/maquillage/",
    "https://www.revolutionbeauty.com/intl/fr/soin/",
    "https://www.revolutionbeauty.com/intl/fr/cheveux/",
    "https://www.revolutionbeauty.com/intl/fr/corps/"
]

# Chemins autorisés pour le crawling (seules ces sections seront explorées)
ALLOWED_PATHS = [
    "/intl/fr/maquillage/",
    "/intl/fr/soin/",
    "/intl/fr/cheveux/",
    "/intl/fr/corps/"
]

# Domaine principal à crawler
DOMAIN = "revolutionbeauty.com"
# Nombre de threads simultanés pour le crawling parallèle
MAX_WORKERS = 15
# Délai en secondes entre chaque requête (pour éviter de surcharger le serveur)
DELAY = 0.15

# En-têtes HTTP pour simuler un navigateur web
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "fr-FR,fr;q=0.9"
}

# ---------------- GLOBALS ---------------- #
# Ensemble des URLs déjà visitées pour éviter les doublons
visited = set()
# Ensemble des URLs de produits déjà sauvegardées
saved = set()
# File d'attente des URLs à crawler
queue = deque()
# Lock pour synchroniser l'accès aux variables partagées entre threads
lock = threading.Lock()

# ---------------- SETUP ---------------- #
def setup():
    """Initialise le dossier data et crée le fichier CSV de sortie avec les en-têtes."""
    os.makedirs("data", exist_ok=True)
    with open("data/urls.csv", "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["url"])

def is_internal(url):
    """Vérifie si l'URL appartient au domaine Revolution Beauty.
    
    Args:
        url (str): URL à vérifier
        
    Returns:
        bool: True si l'URL est interne au domaine
    """
    return DOMAIN in urlparse(url).netloc

def is_allowed_path(url):
    """Vérifie si le chemin de l'URL fait partie des chemins autorisés.
    
    Args:
        url (str): URL à vérifier
        
    Returns:
        bool: True si le chemin est autorisé
    """
    path = urlparse(url).path
    return any(path.startswith(p) for p in ALLOWED_PATHS)

def is_product_page(url):
    """Détermine si une URL correspond à une page produit.
    
    Les pages produits se terminent par .html et sont dans un chemin autorisé.
    
    Args:
        url (str): URL à vérifier
        
    Returns:
        bool: True si c'est une page produit
    """
    return url.endswith(".html") and is_allowed_path(url)

def fetch(url):
    """Récupère le contenu HTML d'une URL.
    
    Ajoute un délai avant la requête pour respecter le serveur.
    
    Args:
        url (str): URL à récupérer
        
    Returns:
        str|None: Contenu HTML de la page ou None en cas d'erreur
    """
    time.sleep(DELAY)
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            return r.text
    except:
        pass
    return None

def extract_links(html, base_url):
    """Extrait tous les liens internes valides d'une page HTML.
    
    Parse le HTML, trouve tous les liens <a>, les convertit en URLs absolues,
    et filtre uniquement ceux qui sont internes et dans les chemins autorisés.
    
    Args:
        html (str): Contenu HTML de la page
        base_url (str): URL de base pour résoudre les liens relatifs
        
    Returns:
        list: Liste des URLs trouvées
    """
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    for a in soup.find_all("a", href=True):
        # Convertit en URL absolue, retire les ancres (#) et slashes finaux
        url = urljoin(base_url, a["href"]).split("#")[0].rstrip("/")
        if is_internal(url) and is_allowed_path(url):
            links.add(url)

    return list(links)

def save_url(url):
    """Sauvegarde une URL de produit dans le fichier CSV.
    
    Thread-safe grâce à l'utilisation d'un lock. Évite les doublons.
    
    Args:
        url (str): URL du produit à sauvegarder
    """
    with lock:
        if url in saved:
            return
        saved.add(url)
        with open("data/urls.csv", "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([url])
        print(f"✓ PRODUCT: {url}")

# ---------------- WORKER ---------------- #
def process(url):
    """Traite une URL : la visite, extrait les liens ou sauvegarde si c'est un produit.
    
    Fonction exécutée par chaque thread worker. Thread-safe.
    
    Args:
        url (str): URL à traiter
        
    Returns:
        list: Liste de nouveaux liens à crawler (vide si page produit)
    """
    # Marque l'URL comme visitée (thread-safe)
    with lock:
        if url in visited:
            return []
        visited.add(url)

    # Récupère le contenu de la page
    html = fetch(url)
    if not html:
        return []

    # Si c'est une page produit, on la sauvegarde et on ne crawle pas plus loin
    if is_product_page(url):
        save_url(url)
        return []

    # Sinon, on extrait les liens pour continuer le crawling
    return extract_links(html, url)

# ---------------- MAIN ---------------- #
def crawl():
    """Fonction principale de crawling.
    
    Lance le processus de crawling multi-threadé :
    1. Initialise le fichier de sortie
    2. Ajoute les URLs de départ à la queue
    3. Lance les workers pour traiter les URLs en parallèle
    4. Collecte les nouveaux liens trouvés et les ajoute à la queue
    5. Continue jusqu'à ce que toutes les URLs soient traitées
    """
    setup()

    # Ajoute les URLs de départ à la queue
    for u in START_URLS:
        queue.append(u)

    # Lance le pool de threads
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = set()

        # Boucle principale : tant qu'il y a des URLs à traiter ou des tâches en cours
        while queue or futures:
            # Soumet de nouvelles tâches tant qu'il y a de la place
            while queue and len(futures) < MAX_WORKERS:
                futures.add(executor.submit(process, queue.popleft()))

            # Collecte les résultats des tâches terminées
            done = set()
            for future in as_completed(futures):
                try:
                    links = future.result()
                    # Ajoute les nouveaux liens à la queue
                    for l in links:
                        with lock:
                            if l not in visited:
                                queue.append(l)
                except:
                    pass
                done.add(future)

            # Retire les tâches terminées
            futures -= done

    # Affiche les statistiques finales
    print("\n✅ FINISHED")
    print(f"Visited URLs: {len(visited)}")
    print(f"Saved product URLs: {len(saved)}")

# ---------------- RUN ---------------- #
if __name__ == "__main__":
    crawl()
