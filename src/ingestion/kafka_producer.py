"""
kafka_producer.py - NYC Taxi verisini Kafka'ya stream eder.

Parquet dosyasını okur ve saniyede ~200 kayıt olarak
'taxi_trips' topic'ine JSON formatında gönderir.

Kullanım (spark-workspace container içinde):
    python src/ingestion/kafka_producer.py
"""

import json
import sys
import time
import os

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PARQUET_FILE,
    PRODUCER_BATCH_SIZE,
    PRODUCER_SEND_INTERVAL,
)
from src.config.schema import TAXI_COLUMNS, DATETIME_COLUMNS


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
