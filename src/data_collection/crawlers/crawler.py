import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ---------------- CONFIG ---------------- #
CATEGORIES = {
    "maquillage": "https://www.revolutionbeauty.com/intl/fr/maquillage/",
    "soin": "https://www.revolutionbeauty.com/intl/fr/soin/",
    "cheveux": "https://www.revolutionbeauty.com/intl/fr/cheveux/",
    "corps": "https://www.revolutionbeauty.com/intl/fr/corps/"
}
DOMAIN = "revolutionbeauty.com"
TARGET_LEVEL = 4
DELAY = 0.2  # Réduit pour accélérer
MAX_WORKERS = 10  # Réduit pour éviter la surcharge

visited = set()
level4_urls = set()  # Track Level 4 URLs separately
lock = threading.Lock()
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

# ---------------- HELPERS ---------------- #

def ensure_data_folder():
    """Créer le dossier data/total_urls s'il n'existe pas"""
    os.makedirs("data/total_urls", exist_ok=True)

def is_internal(url):
    return DOMAIN in urlparse(url).netloc

def depth_of(url, root_path):
    path = urlparse(url).path
    sub = path.replace(root_path, "")
    parts = [p for p in sub.split("/") if p]
    return len(parts) + 1

def fetch(url):
    time.sleep(DELAY)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return r.text
        else:
            print(f"⚠ Status {r.status_code} for {url}")
    except Exception as e:
        print(f"✗ Error fetching {url}: {str(e)[:50]}")
    return None

def extract_links(html, current_url, root_path):
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.find_all("a", href=True):
        url = urljoin(current_url, a["href"]).rstrip("/")
        if not is_internal(url):
            continue
        if not urlparse(url).path.startswith(root_path):
            continue
        if "#" in url:
            continue
        links.append(url)

    return links

def get_title(html):
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""

# ---------------- CSV ---------------- #

ensure_data_folder()

def save_row(csv_file, url, name):
    with lock:
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([url, name])

# ---------------- PROCESS PAGE ---------------- #

def process(url, csv_file, root_path):
    with lock:
        if url in visited:
            return []
        visited.add(url)

    d = depth_of(url, root_path)
    print(f"[Level {d}] Processing: {url}")
    
    if d == TARGET_LEVEL:
        with lock:
            if url not in level4_urls:
                level4_urls.add(url)
                html = fetch(url)
                if html:
                    title = get_title(html)
                    save_row(csv_file, url, title)
                    print(f"✓ Saved Level {d}: {title[:50]}...")
        return []

    if d < TARGET_LEVEL:
        html = fetch(url)
        if html:
            return extract_links(html, url, root_path)
    return []

# ---------------- CRAWL ---------------- #

def crawl_category(name, base_url):
    print(f"\n🚀 START SCRAPING {name.upper()} Level 4 URLs...\n")
    root_path = urlparse(base_url).path
    csv_file = os.path.join("data/total_urls", f"{name}.csv")
    if not os.path.exists(csv_file):
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["url", "name"])

    queue = [base_url]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while queue:
            batch = queue[:MAX_WORKERS]
            queue = queue[MAX_WORKERS:]
            futures = {executor.submit(process, url, csv_file, root_path): url for url in batch}

            for future in as_completed(futures):
                try:
                    links = future.result()
                    for l in links:
                        with lock:
                            if l not in visited:
                                queue.append(l)
                except Exception as e:
                    print(f"Error: {e}")

    print(f"\n✅ DONE {name.upper()} Level 4 URLs")
    print(f"Total Level 4 URLs saved: {len(level4_urls)}")
    print(f"Total pages visited (all levels): {len(visited)}\n")

# ---------------- RUN ---------------- #

if __name__ == "__main__":
    for name, url in CATEGORIES.items():
        # Reset visited and level4_urls for each category
        visited.clear()
        level4_urls.clear()
        crawl_category(name, url)
