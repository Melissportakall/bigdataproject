"""
producer.py - NYC Taxi verisini Kafka'ya stream eder.

Parquet dosyasını okur ve saniyede ~200 kayıt olarak
'taxi_trips' topic'ine JSON formatında gönderir.

Docker Compose ile otomatik çalışır:
    docker-compose up kafka-producer
"""

import json
import sys
import time
import os

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Sabitler ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = "taxi_trips"
PRODUCER_BATCH_SIZE = 200          # saniyede gönderilecek kayıt sayısı
PRODUCER_SEND_INTERVAL = 1.0      # saniye cinsinden gönderim aralığı
PARQUET_FILE = "/app/data/yellow_tripdata_2024-01.parquet"

# Kafka'ya gönderilecek kolonlar
TAXI_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]

# Datetime kolonları (JSON'a çevirirken ISO-8601 string yapılacak)
DATETIME_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


def create_producer(retries: int = 5, wait: int = 5) -> KafkaProducer:
    """Kafka broker'a bağlanana kadar tekrar dener."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                batch_size=16_384,
                linger_ms=10,
            )
            print(f"✓ Kafka broker'a bağlandı: {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"✗ Broker bulunamadı, tekrar deneniyor ({attempt}/{retries})...")
            time.sleep(wait)

    raise ConnectionError("Kafka broker'a bağlanılamadı!")


def load_parquet(path: str) -> pd.DataFrame:
    """Parquet dosyasını okuyup datetime kolonlarını string'e çevirir."""
    print(f"Parquet okunuyor: {path}")
    df = pd.read_parquet(path, columns=TAXI_COLUMNS)

    # 22 Ocak 2024 ve sonrasını filtrele
    cutoff = pd.Timestamp("2024-01-22")
    before = len(df)
    df = df[df["tpep_pickup_datetime"] >= cutoff].reset_index(drop=True)
    print(f"  → Filtre: pickup >= {cutoff.date()} | {before:,} → {len(df):,} satır")

    # Datetime → ISO-8601 string (JSON serializable)
    for col in DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

    # NaN değerleri None'a çevir (JSON null)
    df = df.where(df.notna(), None)

    print(f"  → {len(df):,} satır yüklendi.")
    return df


def stream_to_kafka(producer: KafkaProducer, df: pd.DataFrame) -> None:
    """DataFrame'i batch'ler halinde Kafka'ya gönderir."""
    total_rows = len(df)
    sent = 0
    batch_num = 0

    print(f"\nStreaming başlıyor → topic: '{KAFKA_TOPIC}'")
    print(f"  Hız: {PRODUCER_BATCH_SIZE} kayıt / {PRODUCER_SEND_INTERVAL}s\n")

    try:
        for start in range(0, total_rows, PRODUCER_BATCH_SIZE):
            batch_num += 1
            end = min(start + PRODUCER_BATCH_SIZE, total_rows)
            batch = df.iloc[start:end]

            for _, row in batch.iterrows():
                record = row.to_dict()
                producer.send(KAFKA_TOPIC, value=record)
                sent += 1

            producer.flush()

            pct = sent * 100 / total_rows
            sys.stdout.write(
                f"\r  Batch #{batch_num:,} | Gönderilen: {sent:,}/{total_rows:,} ({pct:.1f}%)"
            )
            sys.stdout.flush()

            # Hız kontrolü – saniyede PRODUCER_BATCH_SIZE kayıt
            time.sleep(PRODUCER_SEND_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n\n⚠ Kullanıcı tarafından durduruldu. Toplam gönderilen: {sent:,}")
    else:
        print(f"\n\n✓ Tamamlandı! Toplam {sent:,} kayıt Kafka'ya gönderildi.")
    finally:
        producer.close()


def main():
    print("=" * 60)
    print("  NYC Taxi → Kafka Producer")
    print("=" * 60)

    producer = create_producer()
    df = load_parquet(PARQUET_FILE)
    stream_to_kafka(producer, df)


if __name__ == "__main__":
    main()
