# test_features.py
from pyspark.sql import SparkSession
import sys

def test():
    spark = SparkSession.builder \
        .appName("FeatureTest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    path = "/home/jovyan/work/data/delta/features"
    df = spark.read.format("delta").load(path)

    print("\n" + "="*50)
    print("🚀 ÖZELLİK TABLOSU TEST SONUÇLARI")
    print("="*50)
    
    print(f"✅ Toplam Kayıt Sayısı: {df.count():,}")
    
    print("\n📊 Yeni Üretilen Özellikler (İlk 10 Satır):")
    df.select(
        "trip_distance", "trip_duration_min", "average_speed", 
        "pickup_hour", "day_of_week", "is_airport_trip", "is_rush_hour"
    ).show(10)

    print("\n📈 Temel İstatistikler:")
    df.describe("trip_duration_min", "average_speed").show()
    
    print("="*50)
    spark.stop()

if __name__ == "__main__":
    test()
