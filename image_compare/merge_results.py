import csv
import glob

files = glob.glob("outputs/**/output_*.csv", recursive=True)

with open("final_output.csv", "w", newline="", encoding="utf-8") as fout:
    writer = csv.writer(fout)

    writer.writerow([
        "product_id",
        "competitor_id",
        "brand_id",
        "competitor_image_url",
        "1sb_image_url",
        "similarity_score",
        "match_type"
    ])

    for file in files:
        with open(file, newline='', encoding='utf-8') as fin:
            reader = csv.reader(fin)
            next(reader, None)
            writer.writerows(reader)