import os
import csv
import requests
from PIL import Image
from io import BytesIO
import torch
import clip
from concurrent.futures import ThreadPoolExecutor

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHUNK_DIR = os.path.join(BASE_DIR, "chunks")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
CACHE_DIR = os.path.join(BASE_DIR, "image_cache")

INPUT_FILE = os.environ.get("INPUT_FILE")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# -------------------------------
# Load CLIP
# -------------------------------
device = "cuda" if torch.cuda.is_available() else "cpu"
model, preprocess = clip.load("ViT-B/32", device=device)

# -------------------------------
# HTTP session (faster)
# -------------------------------
session = requests.Session()

def download_image(url):
    """Download with disk cache"""
    try:
        filename = os.path.join(CACHE_DIR, str(abs(hash(url))) + ".jpg")

        if os.path.exists(filename):
            return Image.open(filename).convert("RGB")

        r = session.get(url, timeout=10)
        if r.status_code == 200:
            img = Image.open(BytesIO(r.content)).convert("RGB")
            img.save(filename)
            return img
    except:
        return None

# -------------------------------
# Batch embedding
# -------------------------------
def get_embeddings(images):
    try:
        inputs = torch.stack([preprocess(img) for img in images]).to(device)
        with torch.no_grad():
            emb = model.encode_image(inputs)
        emb = emb / emb.norm(dim=-1, keepdim=True)
        return emb
    except:
        return None

# -------------------------------
# Main Process
# -------------------------------
def process():
    input_path = os.path.join(CHUNK_DIR, INPUT_FILE)
    output_path = os.path.join(OUTPUT_DIR, f"output_{INPUT_FILE}")

    rows = []
    urls = set()

    # Step 1: Read & collect unique URLs
    with open(input_path, newline='', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))
        for row in reader:
            urls.add(row['competitor_image_url'])
            urls.add(row['1sb_image_url'])
            rows.append(row)

    # Step 2: Download images (parallel)
    url_to_image = {}

    def load(url):
        img = download_image(url)
        return url, img

    with ThreadPoolExecutor(max_workers=10) as executor:
        for url, img in executor.map(load, urls):
            if img:
                url_to_image[url] = img

    # Step 3: Batch embedding
    url_list = list(url_to_image.keys())
    images = [url_to_image[url] for url in url_list]

    embeddings = {}
    batch_size = 32

    for i in range(0, len(images), batch_size):
        batch_imgs = images[i:i+batch_size]
        batch_urls = url_list[i:i+batch_size]

        emb = get_embeddings(batch_imgs)
        if emb is None:
            continue

        for j, url in enumerate(batch_urls):
            embeddings[url] = emb[j].unsqueeze(0)

    # Step 4: Compare
    results = []

    for row in rows:
        comp_url = row['competitor_image_url']
        sb_url = row['1sb_image_url']

        emb1 = embeddings.get(comp_url)
        emb2 = embeddings.get(sb_url)

        if emb1 is not None and emb2 is not None:
            score = torch.cosine_similarity(emb1, emb2).item()
            score = round(score, 4)
        else:
            score = -1

        # match flag
        if score == -1:
            match = "ERROR"
        elif score >= 0.90:
            match = "EXACT"
        elif score >= 0.80:
            match = "SIMILAR"
        elif score >= 0.65:
            match = "PARTIAL"
        else:
            match = "DIFFERENT"

        results.append([
            row['product_id'],
            row['competitor_id'],
            row['brand_id'],
            comp_url,
            sb_url,
            score,
            match
        ])

    # Step 5: Save
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "product_id",
            "competitor_id",
            "brand_id",
            "competitor_image_url",
            "1sb_image_url",
            "similarity_score",
            "match_type"
        ])
        writer.writerows(results)


if __name__ == "__main__":
    process()