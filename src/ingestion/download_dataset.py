"""
download_dataset.py - NYC Yellow Taxi 2024-01 Parquet verisini indirir.

Kullanım (container içinde):
    python src/ingestion/download_dataset.py
"""

import os
import sys
import urllib.request

DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
FILE_NAME = "yellow_tripdata_2024-01.parquet"

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")


def download():
    os.makedirs(DATA_DIR, exist_ok=True)
    dest = os.path.join(DATA_DIR, FILE_NAME)

    if os.path.exists(dest):
        size_mb = os.path.getsize(dest) / (1024 * 1024)
        print(f"Dosya zaten mevcut: {dest} ({size_mb:.1f} MB) - indirme atlanıyor.")
        return

    print(f"Indiriliyor: {DATA_URL}")

    def progress(block, block_size, total):
        pct = min(block * block_size * 100 / total, 100) if total > 0 else 0
        sys.stdout.write(f"\r  %{pct:.1f}")
        sys.stdout.flush()

    urllib.request.urlretrieve(DATA_URL, dest, reporthook=progress)
    size_mb = os.path.getsize(dest) / (1024 * 1024)
    print(f"\nTamamlandi: {dest} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    download()
