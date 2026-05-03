# spark_streaming.py
import sys
sys.path.insert(0, "/home/jovyan/work/src")

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    SPARK_APP_NAME,
    DELTA_OUTPUT_PATH,
    DELTA_CHECKPOINT_PATH
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, unix_timestamp,
    hour, dayofweek, when
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    IntegerType, TimestampType
)

# ── Spark Başlat ──────────────────────────────────────────
# NOT: Spark 3.5.0 imajı ile Delta 2.4.0 uyum sorunu (TypeError: 'JavaPackage' object is not callable) 
# çıkardığı için versiyonlar güncellendi. Eski sürümler: delta-core:2.4.0 ve kafka:3.4.0 idi.
spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark başlatıldı.")

# ── Kafka'dan Gelecek JSON'un Şeması ─────────────────────
# Kafka'dan gelen her mesaj JSON formatında
# Hangi alanlar var, hangi tipte olduklarını tanımlıyoruz
taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

print("✅ Şema tanımlandı.")


# ── Kafka'dan Stream Olarak Oku (BRONZE) ─────────────────
print("📡 Kafka'dan veri okunuyor...")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka'dan gelen veri "value" kolonunda binary olarak geliyor
# Önce string'e çeviriyoruz, sonra JSON'u parse ediyoruz
bronze_df = raw_stream.select(
    from_json(
        col("value").cast("string"),
        taxi_schema
    ).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

print("✅ Bronze katman hazır.")

# Bronze'u Delta Lake'e yaz
bronze_query = bronze_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", DELTA_CHECKPOINT_PATH + "/bronze") \
    .outputMode("append") \
    .start(DELTA_OUTPUT_PATH + "/bronze")

print("✅ Bronze Delta Lake'e yazılıyor...")

# ── Temizlik (SILVER) ─────────────────────────────────────
silver_df = bronze_df \
    .filter(
        (col("trip_distance") > 0) &
        (col("total_amount") > 0) &
        (col("total_amount") < 500) &
        (col("passenger_count") > 0) &
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime").isNotNull())
    ) \
    .withColumn(
        "tpep_pickup_datetime",
        col("tpep_pickup_datetime").cast(TimestampType())
    ) \
    .withColumn(
        "tpep_dropoff_datetime",
        col("tpep_dropoff_datetime").cast(TimestampType())
    ) \
    .dropDuplicates(["tpep_pickup_datetime", "PULocationID", "DOLocationID", "total_amount"])

print("✅ Silver katman hazır.")

# Silver'ı Delta Lake'e yaz
silver_query = silver_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", DELTA_CHECKPOINT_PATH + "/silver") \
    .outputMode("append") \
    .start(DELTA_OUTPUT_PATH + "/silver")

print("✅ Silver Delta Lake'e yazılıyor...")

# ── Tüm Stream'lerin Bitmesini Bekle ─────────────────────
print("🚀 Pipeline çalışıyor, Kafka'dan veri bekleniyor...")
spark.streams.awaitAnyTermination()