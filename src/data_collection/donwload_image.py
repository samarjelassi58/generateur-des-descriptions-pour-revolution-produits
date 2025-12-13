"""Script de téléchargement d'images de produits.

Ce script télécharge toutes les images des produits depuis leurs URLs en ligne
et les stocke localement. Les URLs dans le fichier CSV sont ensuite remplacées
par les chemins locaux des images téléchargées.

Fonctionnalités:
- Téléchargement progressif avec affichage de la progression
- Organisation des images par dossiers (un dossier par produit)
- Gestion des erreurs et timeouts
- Sauvegarde progressive du CSV (ne perd pas la progression en cas d'interruption)
- Nettoyage des noms de fichiers pour éviter les caractères invalides

Usage:
    python donwload_image.py

Entrée:
    data/produits.csv - Fichier contenant les URLs des images (colonne 'images')

Sortie:
    data/produits_local.csv - Fichier avec chemins locaux des images
    data/images/ - Dossier contenant toutes les images téléchargées
"""

import os
import pandas as pd
import requests
import re

# ================= CONFIGURATION =================
# Fichier CSV d'entrée contenant les URLs des images
input_file = "data/produits.csv"
# Fichier CSV de sortie avec les chemins locaux
output_file = "data/produits_local.csv"

# Crée le dossier principal pour stocker toutes les images
os.makedirs("data/images", exist_ok=True)

# Charge le fichier CSV contenant les produits et leurs URLs d'images
df = pd.read_csv(input_file)

# Nom de la colonne contenant les URLs des images (séparées par |)
image_col = "images"

# ================= FONCTIONS UTILITAIRES =================
def clean_name(name):
    """Nettoie un nom de produit pour l'utiliser comme nom de fichier/dossier.
    
    Remplace tous les caractères non alphanumériques (sauf - et _) par des underscores
    pour éviter les problèmes avec les systèmes de fichiers.
    
    Args:
        name (str): Nom du produit à nettoyer
        
    Returns:
        str: Nom nettoyé utilisable pour les fichiers/dossiers
        
    Exemple:
        >>> clean_name("Revolution Bouncy Blur Blush!")
        'Revolution_Bouncy_Blur_Blush_'
    """
    return re.sub(r'[^a-zA-Z0-9_-]', '_', name)

# ================= TÉLÉCHARGEMENT DES IMAGES =================
# Liste pour stocker les nouveaux chemins locaux de toutes les images
updated_paths = []

# Calcule le nombre total de produits pour l'affichage de la progression
total_products = len(df)
print(f"Début du téléchargement pour {total_products} produits...\n")

# Traite chaque produit un par un
for index, row in df.iterrows():
    # Affiche le produit en cours de traitement
    print(f"Produit {index + 1}/{total_products}: {row['name'][:50]}...") # type: ignore
    
    # Récupère toutes les URLs d'images pour ce produit (séparées par |)
    urls = str(row[image_col]).split("|")
    local_paths = []  # Chemins locaux pour ce produit

    # Crée un dossier unique pour ce produit
    # Utilise le nom nettoyé + l'index pour garantir l'unicité
    product_name = clean_name(str(row['name']))
    product_folder = os.path.join("data/images", f"{product_name}_{index}")
    os.makedirs(product_folder, exist_ok=True)
    
    # Télécharge chaque image du produit
    for i, url in enumerate(urls, start=1):
        if url.strip():  # Ignore les URLs vides
            try:
                print(f"  - Téléchargement image {i}/{len(urls)}...", end=" ")
                
                # Télécharge l'image avec un timeout de 10 secondes
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    # Extrait l'extension du fichier depuis l'URL
                    # Retire les paramètres (?...) et les ancres (#...)
                    ext = url.split("?")[0].split(".")[-1].split("#")[0]
                    
                    # Crée le chemin complet du fichier local
                    filename = os.path.join(product_folder, f"{product_name}_{i}.{ext}")
                    
                    # Sauvegarde l'image sur le disque
                    with open(filename, "wb") as f:
                        f.write(response.content)
                    
                    # Ajoute le chemin local à la liste
                    local_paths.append(filename)
                    print("✓")
                else:
                    print(f"✗ (Status: {response.status_code})")
            except requests.Timeout:
                print("✗ (Timeout)")
            except Exception as e:
                print(f"✗ ({type(e).__name__})")
    
    # Ajoute les chemins locaux pour ce produit (séparés par |)
    updated_paths.append("|".join(local_paths))
    print(f"  → {len(local_paths)} image(s) téléchargée(s)\n")
    
    # ================= SAUVEGARDE PROGRESSIVE =================
    # Sauvegarde le CSV après chaque produit pour ne pas perdre la progression
    # en cas d'interruption (Ctrl+C, erreur, timeout, etc.)
    df_temp = df.copy()
    df_temp.loc[:index, image_col] = updated_paths
    df_temp.to_csv(output_file, index=False)
    print(f"  ✓ CSV mis à jour ({index + 1}/{total_products} produits)\n") # type: ignore

# ================= SAUVEGARDE FINALE =================
# Met à jour la colonne images avec tous les chemins locaux
df[image_col] = updated_paths

# Sauvegarde finale du CSV complet
df.to_csv(output_file, index=False)

# Affiche le récapitulatif final
print(f"\n{'='*60}")
print(f"✓ TERMINÉ !")
print(f"✓ Fichier final créé : {output_file}")
print(f"✓ Total: {sum(len(p.split('|')) for p in updated_paths if p)} images téléchargées")
print(f"{'='*60}")
