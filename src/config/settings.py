"""
settings.py - Proje genelinde kullanılan sabitler.

Docker-compose ağında servis adları hostname olarak çalışır:
  kafka:29092  (container-içi), localhost:9092 (host'tan erişim)
"""

import os

# ── Kafka ────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = "taxi_trips"
PRODUCER_BATCH_SIZE = 200          # saniyede gönderilecek kayıt sayısı
PRODUCER_SEND_INTERVAL = 1.0      # saniye cinsinden gönderim aralığı

# ── Veri Yolları ─────────────────────────────────────────
# Container içinde çalışırken /home/jovyan/work altına mount ediliyor
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
PARQUET_FILE = os.path.join(DATA_DIR, "yellow_tripdata_2024-01.parquet")

# ── Delta Lake ───────────────────────────────────────────
DELTA_OUTPUT_PATH = os.path.join(DATA_DIR, "delta", "taxi_trips")
DELTA_CHECKPOINT_PATH = os.path.join(DATA_DIR, "delta", "_checkpoints")

# ── MLflow ───────────────────────────────────────────────
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5001")
MLFLOW_EXPERIMENT_NAME = "nyc-taxi-models"

# ── Spark ────────────────────────────────────────────────
SPARK_APP_NAME = "NYC-Taxi-Pipeline"
