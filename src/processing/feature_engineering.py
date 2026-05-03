# feature_engineering.py
import os
import sys

# Proje kök dizinini ekle
sys.path.insert(0, "/home/jovyan/work/src")

from config.settings import (
    SPARK_APP_NAME,
    DELTA_OUTPUT_PATH,
    FEATURES_OUTPUT_PATH
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, hour, dayofweek, when, round
)

def main():
    # ── Spark Başlat ──────────────────────────────────────────
    spark = SparkSession.builder \
        .appName(f"{SPARK_APP_NAME}-FeatureEngineering") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark başlatıldı.")

    # ── Silver Tablosunu Oku ──────────────────────────────────
    silver_path = f"{DELTA_OUTPUT_PATH}/silver"
    print(f"📖 Silver tablosu okunuyor: {silver_path}")
    
    if not os.path.exists(silver_path.replace("/home/jovyan/work", "/Users/melisportakal/Desktop/bigdata")):
        # Not: Container içindeki yol ile local yol farklı olabilir, kontrol ediyoruz.
        pass

    silver_df = spark.read.format("delta").load(silver_path)

    # ── Özellik Mühendisliği (Feature Engineering) ────────────
    print("🛠️ Özellikler üretiliyor...")

    features_df = silver_df \
        .withColumn(
            # 1. Yolculuk Süresi (Dakika)
            "trip_duration_min",
            round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)
        ) \
        .filter(col("trip_duration_min") > 0) \
        .withColumn(
            # 2. Ortalama Hız (mil/saat)
            "average_speed",
            round(col("trip_distance") / (col("trip_duration_min") / 60), 2)
        ) \
        .withColumn(
            # 3. Alınış Saati
            "pickup_hour",
            hour("tpep_pickup_datetime")
        ) \
        .withColumn(
            # 4. Haftanın Günü (1:Pazar, 2:Pazartesi... Spark varsayılanı)
            "day_of_week",
            dayofweek("tpep_pickup_datetime")
        ) \
        .withColumn(
            # 5. Havaalanı Yolculuğu mu? (Ratecode 2=JFK, 3=Newark veya Airport_fee varsa)
            "is_airport_trip",
            when((col("RatecodeID").isin(2.0, 3.0)) | (col("Airport_fee") > 0), 1).otherwise(0)
        ) \
        .withColumn(
            # 6. Yoğun Saat mi? (08-10 veya 16-19 arası)
            "is_rush_hour",
            when((col("pickup_hour").isin(8, 9, 16, 17, 18, 19)), 1).otherwise(0)
        )

    # Gereksiz olabilecek veya çok sapan hızları temizle (Örn: 150 mil/saat üstü hatalı olabilir)
    features_df = features_df.filter(col("average_speed") < 150)

    print("✅ 6 Yeni özellik başarıyla eklendi.")
    features_df.select(
        "trip_distance", "trip_duration_min", "average_speed", 
        "pickup_hour", "day_of_week", "is_airport_trip", "is_rush_hour", "total_amount"
    ).show(5)

    # ── Features Tablosuna Kaydet (Gold Katmanı) ──────────────
    print(f"💾 Özellikler kaydediliyor: {FEATURES_OUTPUT_PATH}")
    
    features_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(FEATURES_OUTPUT_PATH)

    print("🚀 Adım 5 başarıyla tamamlandı: Özellik tablosu Delta Lake'e kaydedildi.")
    spark.stop()

if __name__ == "__main__":
    main()
