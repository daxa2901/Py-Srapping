import os
import csv
import json
from collections import defaultdict
import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, "input.csv")
CHUNK_DIR = os.path.join(BASE_DIR, "chunks")
MATRIX_FILE = os.path.join(BASE_DIR, "matrix.json")

SHEET_URL = "https://docs.google.com/spreadsheets/d/1PgyuaTMU7gwpUr_I5nuWC5-dY70Ya4w-XGVjVKt0rSY/export?format=csv"


def download_input():
    r = requests.get(SHEET_URL)
    with open(INPUT_FILE, "wb") as f:
        f.write(r.content)


def create_chunks():
    os.makedirs(CHUNK_DIR, exist_ok=True)

    brand_groups = defaultdict(list)

    with open(INPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            brand_id = row['brand_id']
            brand_groups[brand_id].append(row)

    matrix = []

    for brand_id, rows in brand_groups.items():
        filename = f"brand_{brand_id}.csv"
        filepath = os.path.join(CHUNK_DIR, filename)

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        matrix.append({
            "brand_id": brand_id,
            "file": filename
        })

    with open(MATRIX_FILE, "w") as f:
        json.dump({"include": matrix}, f, indent=2)


if __name__ == "__main__":
    download_input()
    create_chunks()