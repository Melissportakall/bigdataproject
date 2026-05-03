# Adım 5: Özellik Mühendisliği (Feature Engineering)

Bu aşamada, `silver` katmanındaki temizlenmiş NYC Taxi verilerinden, makine öğrenmesi modellerinin tahmin başarısını artıracak **6 yeni özellik** üretilmiştir.

## Üretilen Özellikler ve Seçilme Nedenleri

| Özellik Adı | Açıklama | İş Mantığı / Neden Seçildi? (Nasıl Üretildi?) |
| :--- | :--- | :--- |
| **trip_duration_min** | Yolculuğun dakika cinsinden süresi | Ücret tahmini için temel faktördür. **(tpep_dropoff_datetime - tpep_pickup_datetime)** |
| **average_speed** | Mil / Saat cinsinden ortalama hız | Trafik yoğunluğunu temsil eder. **(trip_distance / trip_duration_min)** |
| **pickup_hour** | Yolculuğun başladığı saat (0-23) | Günün saatine göre talep ve ek ücretler değişir. **(hour of pickup_datetime)** |
| **day_of_week** | Haftanın günü (1: Pazar - 7: Cumartesi) | Hafta içi ve sonu trafik profilini ayırır. **(dayofweek of pickup_datetime)** |
| **is_airport_trip** | Havaalanı yolculuğu mu? (Binary: 0/1) | Havaalanı tarifeleri ve bahşişleri farklıdır. **(RatecodeID 2/3 veya Airport_fee > 0)** |
| **is_rush_hour** | Yoğun saat trafiği mi? (Binary: 0/1) | Congestion surcharge ek ücretini belirler. **(pickup_hour in 8-10, 16-19)** |

## Uygulama Detayları
- **Teknoloji:** Apache Spark (PySpark) & Delta Lake
- **Girdi:** `data/delta/taxi_trips/silver`
- **Çıktı:** `data/delta/features` (Gold Katmanı)
- **Filtreleme:** Hatalı veri olmaması adına, süresi 0 veya negatif olan ve hızı 150 mil/saat'in üzerinde olan kayıtlar elenmiştir.

## Veri Doğrulama
Yapılan testlerde 380.000+ kayıt için özelliklerin başarıyla üretildiği ve veride `null` değer bulunmadığı teyit edilmiştir.
