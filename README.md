# bigdataproject

Uçtan Uca Proje Mimarisi

1. Konteyner Ortamı (Docker):
Tüm sistem bileşenleri birbirleriyle aynı ağ (network) üzerinde konuşacak şekilde tek bir docker-compose.yml ile ayağa kaldırılacak. Kafka, Spark, MLflow ve veri tabanları izole ama bağlantılı çalışacak.

2. Streaming Veri Üretimi (Apache Kafka):
İndirdiğimiz NYC Taxi verisini okuyan bir Python betiği yazacağız. Bu betik, gerçek bir taksi durağının sisteminden anlık veri akıyormuş gibi saniyede 200 yolculuk verisini Kafka'daki bir "taxi_trips" topic'ine fırlatacak.

3. Veri İşleme (Apache Spark Structured Streaming):
Spark, Kafka'daki bu topic'i sürekli dinleyecek. Gelen ham veriyi anlık olarak alıp temizleyecek (boş değerleri atma, tarih formatlarını düzenleme, hatalı koordinatları filtreleme) ve makine öğrenmesine hazır hale getirecek.

4. Tahmin ve Makine Öğrenmesi (Spark MLlib):
Spark, temizlediği veri üzerinden önceden eğittiğimiz modelleri kullanarak anlık tahminler yapacak.

Sınıflandırma: Bu yolcu bahşiş bırakır mı bırakmaz mı?

Regresyon: Bu yolculuğun toplam ücreti ne kadar tutacak?

5. Depolama (Delta Lake):
Hocanın özellikle istediği Delta Lake, veriyi hem güvenli hem de versiyonlu tutmamızı sağlar. Spark, işlediği ve tahmin sonuçlarını eklediği veriyi anlık olarak klasik bir veritabanına değil, Delta formatında bir dizine (Data Lake) yazacak. Dashboard'lar veriyi bu Delta tablolarından okuyacak.

6. Deney Takibi (MLflow):
Projenin en can alıcı akademik kısmı burası. Çeşitli modeller (Logistic Regression, Random Forest vb.) denerken her bir modelin başarı oranını (Accuracy, RMSE), kullandığımız parametreleri ve modelin kendisini MLflow'a kaydedeceğiz. Sunum sırasında hocaya "MLflow arayüzüne bakın, 5 farklı model denedik ve en iyisi şu parametrelerle çalışan Random Forest oldu" diyebileceksin.

src/
├── config/
│   ├── __init__.py
│   ├── settings.py          # Kafka, Spark, MLflow, Delta Lake sabitleri
│   └── schema.py            # NYC Taxi veri şeması tanımları
├── ingestion/
│   ├── __init__.py
│   ├── download_dataset.py  # NYC Taxi parquet verisini indir
│   └── kafka_producer.py    # Veriyi Kafka topic'ine stream et
├── processing/
│   ├── __init__.py
│   ├── spark_streaming.py   # Kafka → temizle → Delta Lake pipeline
│   └── data_cleaning.py     # Veri temizleme & feature engineering
└── ml/
    ├── __init__.py
    ├── train_models.py       # MLlib model eğitimi + MLflow loglama
    └── streaming_inference.py # Eğitilmiş modelle anlık tahmin