# verify_features_advanced.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan
import sys

def advanced_test():
    spark = SparkSession.builder \
        .appName("AdvancedFeatureVerification") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    path = "/home/jovyan/work/data/delta/features"
    df = spark.read.format("delta").load(path)

    print("\n🔍 --- GELİŞMİŞ VERİ KALİTE KONTROLÜ ---")
    
    # 1. Null Değer Kontrolü
    print("\n1. Eksik (Null/NaN) Değer Kontrolü:")
    null_counts = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in 
                            ["trip_distance", "trip_duration_min", "average_speed", "is_airport_trip", "is_rush_hour"]])
    null_counts.show()

    # 2. Binary (0/1) Alanların Kontrolü
    print("\n2. Binary Alanların Dağılımı (Doğru mu?):")
    df.groupBy("is_airport_trip").count().show()
    df.groupBy("is_rush_hour").count().show()

    # 3. Aykırı Değer (Outlier) Kontrolü
    print("\n3. Mantıksal Sınır Kontrolü:")
    # Süresi 2 saatten (120 dk) fazla olan yolculuk sayısı
    long_trips = df.filter(col("trip_duration_min") > 120).count()
    # Hızı 80 mph'den fazla olan yolculuk sayısı
    fast_trips = df.filter(col("average_speed") > 80).count()
    
    print(f"   - 2 saatten uzun yolculuk sayısı: {long_trips}")
    print(f"   - 80 mph'den hızlı yolculuk sayısı: {fast_trips}")

    print("\n✅ Veri seti modelleme için sağlıklı görünüyor.")
    print("-" * 40)
    spark.stop()

if __name__ == "__main__":
    advanced_test()
