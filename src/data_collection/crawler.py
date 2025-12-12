import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
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
DELAY = 0.1          # Petit délai pour ne pas saturer le serveur
MAX_WORKERS = 20      # Nombre de threads

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

# ---------------- HELPERS ---------------- #
lock = threading.Lock()  # Pour visited et CSV

def ensure_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

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
        r = requests.get(url, headers=HEADERS, timeout=5)
        if r.status_code == 200:
            return r.text
    except:
        return None
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

def save_row(csv_file, url, name):
    with lock:
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([url, name])

# ---------------- PROCESS ---------------- #
def process(url, csv_file, root_path, visited, level4_urls):
    with lock:
        if url in visited:
            return []
        visited.add(url)

    d = depth_of(url, root_path)

    if d == TARGET_LEVEL:
        with lock:
            if url not in level4_urls:
                level4_urls.add(url)
                print(f"[Level {d}] {url}")
        html = fetch(url)
        if html:
            title = get_title(html)
            save_row(csv_file, url, title)
        return []

    elif d < TARGET_LEVEL:
        html = fetch(url)
        if html:
            return extract_links(html, url, root_path)
    return []

# ---------------- CRAWL ---------------- #
def crawl_category(name, base_url):
    print(f"\n🚀 START SCRAPING {name.upper()} Level 4 URLs...\n")

    folder = "data/urls"
    ensure_folder(folder)
    csv_file = os.path.join(folder, f"{name}.csv")
    if not os.path.exists(csv_file):
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["url", "name"])

    visited = set()
    level4_urls = set()
    root_path = urlparse(base_url).path
    queue = deque([base_url])

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = set()
        while queue or futures:
            # Submit new tasks
            while queue and len(futures) < MAX_WORKERS:
                url = queue.popleft()
                future = executor.submit(process, url, csv_file, root_path, visited, level4_urls)
                futures.add(future)

            # Process completed tasks
            done, _ = as_completed(futures, timeout=60), futures.difference_update
            for future in list(futures):
                try:
                    links = future.result(timeout=10)
                    futures.remove(future)
                    for l in links:
                        if l not in visited:
                            queue.append(l)
                except:
                    futures.remove(future)

    print(f"\n✅ DONE {name.upper()} Level 4 URLs")
    print(f"Total Level 4 URLs saved: {len(level4_urls)}")
    print(f"Total pages visited (all levels): {len(visited)}\n")

# ---------------- RUN ---------------- #
if __name__ == "__main__":
    for name, url in CATEGORIES.items():
        crawl_category(name, url)
