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
