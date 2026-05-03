# NYC Yellow Taxi — Uçtan Uca Büyük Veri Pipeline'ı

Büyük Veri Analizine Giriş Dönem Projesi  
Dr. Ayşe Gül Eker — 2025-2026 Bahar Dönemi

## Proje Hakkında

Bu projede NYC Yellow Taxi 2024 Ocak verisi kullanılarak uçtan uca bir büyük veri pipeline'ı kurulmuştur:

- **Docker** ile konteynerize ortam
- **Apache Kafka** ile gerçek zamanlı veri akışı simülasyonu
- **Apache Spark Structured Streaming** ile veri işleme
- **Delta Lake** ile ACID uyumlu depolama (Bronze/Silver/Gold)
- **Spark MLlib + MLflow** ile makine öğrenmesi ve deney takibi
- **Matplotlib** ile görselleştirme ve dashboard

## Proje Yapısı

```
bigdataproject/
├── docker-compose.yml           # Tüm servisleri ayağa kaldırır
├── spark-env/
│   └── Dockerfile               # PySpark + Jupyter Lab imajı
├── kafka-producer/
│   ├── Dockerfile               # Hafif Python imajı
│   ├── producer.py              # Kafka'ya veri stream eden betik
│   └── requirements.txt         # Producer bağımlılıkları
├── data/                        # Veri dosyaları (.gitignore)
├── mlruns/                      # MLflow deneyleri (.gitignore)
└── notebooks/
    ├── 01_docker_setup.ipynb        # Docker ortamı dokümantasyonu
    ├── 02_kafka_streaming.ipynb     # Kafka producer test & izleme
    ├── 03_spark_streaming.ipynb     # Spark Streaming → Delta Lake
    ├── 04_eda.ipynb                 # Keşifsel Veri Analizi
    ├── 05_feature_engineering.ipynb # Özellik Mühendisliği
    ├── 06_ml_models.ipynb           # ML model eğitimi + MLflow
    └── 07_dashboard.ipynb           # Görselleştirme & Dashboard
```

## Servisler ve Portlar

| Servis | Port | Açıklama |
|--------|------|----------|
| Kafka UI | [localhost:8080](http://localhost:8080) | Topic, mesaj ve consumer izleme |
| Jupyter Lab | [localhost:8888](http://localhost:8888) | Spark notebook'ları |
| MLflow | [localhost:5001](http://localhost:5001) | Model deneyleri |
| Spark UI | [localhost:4040](http://localhost:4040) | Spark job'ları (Spark çalışınca aktif) |

## Hızlı Başlangıç

### 1. Veri Setini İndirin

```bash
# NYC Yellow Taxi 2024-01 (~50 MB)
mkdir -p data
curl -o data/yellow_tripdata_2024-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

### 2. Servisleri Başlatın

```bash
docker-compose up -d --build
```

### 3. Notebook'ları Çalıştırın

[http://localhost:8888](http://localhost:8888) adresinden Jupyter Lab'a erişin ve notebook'ları sırasıyla çalıştırın.

## Notebook'lar

| # | Notebook | Açıklama |
|---|----------|----------|
| 01 | Docker Setup | Docker mimarisi, servisler ve çalıştırma talimatları |
| 02 | Kafka Streaming | Producer yapısı, mesaj test etme, topic istatistikleri |
| 03 | Spark Streaming | Kafka → Spark → Delta Lake (Bronze/Silver katmanlar) |
| 04 | EDA | Keşifsel veri analizi, dağılımlar, zaman serisi trendleri |
| 05 | Feature Engineering | ML modeline beslenecek özelliklerin üretimi |
| 06 | ML Models | 5 model eğitimi, karşılaştırma, MLflow entegrasyonu |
| 07 | Dashboard | Performans grafikleri, feature importance, dağılımlar |

## Teknolojiler

| Katman | Teknoloji |
|--------|-----------|
| Konteynerizasyon | Docker, Docker Compose |
| Veri Üretimi | Apache Kafka + Python Producer |
| Veri İşleme | Apache Spark (PySpark) Structured Streaming |
| Depolama | Delta Lake (Bronze / Silver / Gold) |
| Makine Öğrenmesi | Spark MLlib + MLflow |
| Görselleştirme | Matplotlib / Seaborn |