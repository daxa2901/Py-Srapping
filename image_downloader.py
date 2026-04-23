import os
import requests
import pandas as pd
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

SHEET_URL = "https://docs.google.com/spreadsheets/d/1GpvmdKv1mQa7mqKFzNI4fgwXRxAhz7bUklS2J0EgSGI/export?format=csv"

DOWNLOAD_DIR = "images"
BATCH_SIZE = 10
MAX_WORKERS = 10  # parallel downloads

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def get_file_extension(url):
    path = urlparse(url).path
    ext = os.path.splitext(path)[1]
    return ext if ext else ".jpg"


def download_image(row):
    url = str(row['image_url']).strip()
    upc = str(row['upc']).strip()

    if not url:
        return None

    ext = get_file_extension(url)
    filename = os.path.join(DOWNLOAD_DIR, f"{upc}{ext}")

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        with open(filename, 'wb') as f:
            f.write(response.content)

        print(f"Downloaded: {filename}")
        return filename

    except Exception as e:
        print(f"Failed: {url} | Error: {e}")
        return None


def process_batch(df_batch):
    files = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_image, row) for _, row in df_batch.iterrows()]

        for future in as_completed(futures):
            result = future.result()
            if result:
                files.append(result)

    return files


def main():
    df = pd.read_csv(SHEET_URL)

    if 'image_url' not in df.columns or 'upc' not in df.columns:
        print("Missing required columns")
        return

    all_files = []

    # Process in batches
    for i in range(0, len(df), BATCH_SIZE):
        print(f"\nProcessing batch {i} to {i + BATCH_SIZE}")
        batch_df = df.iloc[i:i + BATCH_SIZE]
        batch_files = process_batch(batch_df)
        all_files.extend(batch_files)

if __name__ == "__main__":
    main()