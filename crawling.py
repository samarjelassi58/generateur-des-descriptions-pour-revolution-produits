import asyncio
import csv
import os
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler

# ---------------- CONFIG ---------------- #
CATEGORIES = {
    "maquillage": "https://www.revolutionbeauty.com/intl/fr/maquillage/",
    "soin": "https://www.revolutionbeauty.com/intl/fr/soin/",
    "cheveux": "https://www.revolutionbeauty.com/intl/fr/cheveux/",
    "corps": "https://www.revolutionbeauty.com/intl/fr/corps/"
}

TARGET_LEVEL = 4
OUTPUT_DIR = "data/urls"

# ---------------- HELPERS ---------------- #
def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def save_csv(file_path, data):
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["url", "title"])
        for row in data:
            writer.writerow([row["url"], row["title"]])

# ---------------- CRAWL ---------------- #
async def crawl_category(name, url):
    ensure_dir(OUTPUT_DIR)
    output_file = os.path.join(OUTPUT_DIR, f"{name}.csv")

    async with AsyncWebCrawler(headers={"User-Agent": "Mozilla/5.0"}) as crawler:
        level4_data = []
        async for page in crawler.arun(url=url):
            path_parts = page.url.replace(urlparse(url).path, "").strip("/").split("/")
            if len(path_parts) == TARGET_LEVEL - 1:
                level4_data.append({"url": page.url, "title": page.title or ""})

        save_csv(output_file, level4_data)
        print(f"[{name}] URLs level 4 sauvegardées dans {output_file}, total: {len(level4_data)}")

# ---------------- RUN ---------------- #
async def main():
    tasks = [crawl_category(name, url) for name, url in CATEGORIES.items()]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
