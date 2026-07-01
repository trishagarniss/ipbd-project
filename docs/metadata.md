# Metadata Tabel Database

## Database: `aqi_db`

### 1. `raw_measurements`
Data mentah dari Kafka stream (producer → topic `air-quality-raw`).

| Kolom            | Tipe           | Deskripsi                                 |
|------------------|----------------|-------------------------------------------|
| id               | BIGSERIAL (PK) | Auto-increment ID                         |
| station_id       | VARCHAR(10)    | Kode stasiun (SKA1–SKA5)                  |
| station_name     | VARCHAR(50)    | Nama stasiun                              |
| region           | VARCHAR(50)    | Kecamatan                                 |
| latitude         | FLOAT          | Koordinat latitud                         |
| longitude        | FLOAT          | Koordinat longitud                        |
| timestamp        | TIMESTAMPTZ    | Waktu pengukuran                          |
| pm25             | FLOAT          | PM2.5 (µg/m³)                             |
| pm10             | FLOAT          | PM10 (µg/m³)                              |
| co               | FLOAT          | CO (µg/m³)                                |
| no2              | FLOAT          | NO2 (µg/m³)                               |
| so2              | FLOAT          | SO2 (µg/m³)                               |
| o3               | FLOAT          | O3 (µg/m³)                                |
| uv_index         | FLOAT          | UV Index                                  |
| ispu             | FLOAT          | Indeks Standar Pencemar Udara (ISPU)      |
| ispu_category    | VARCHAR(20)    | Kategori ISPU (Baik–Berbahaya)            |
| temperature      | FLOAT          | Suhu (°C)                                 |
| humidity         | FLOAT          | Kelembaban relatif (%)                    |
| wind_speed       | FLOAT          | Kecepatan angin (km/h)                    |
| precipitation    | FLOAT          | Curah hujan (mm)                          |
| precip_prob      | FLOAT          | Probabilitas hujan (%)                    |
| weather_code     | FLOAT          | WMO weather code                          |
| cloud_cover      | FLOAT          | Tutupan awan (%)                          |
| created_at       | TIMESTAMPTZ    | Waktu insert ke database                  |

**Sumber:** Kafka topic `air-quality-raw` (producer streaming)  
**Retensi:** 90 hari (via cleanup script)  
**Ukuran estimasi:** ~5.5 MB/bulan (5 stasiun × 1 record/menit)

### 2. `stream_agg`
Agregasi window 10 menit (slide 5 menit) dari Spark Structured Streaming.

| Kolom             | Tipe           | Deskripsi                          |
|-------------------|----------------|-------------------------------------|
| id                | BIGSERIAL (PK) | Auto-increment                     |
| station_id        | VARCHAR(10)    | Kode stasiun                       |
| window_start      | TIMESTAMPTZ    | Awal window                        |
| window_end        | TIMESTAMPTZ    | Akhir window                       |
| pm25_avg          | FLOAT          | Rata-rata PM2.5                    |
| pm10_avg          | FLOAT          | Rata-rata PM10                     |
| co_avg            | FLOAT          | Rata-rata CO                       |
| no2_avg           | FLOAT          | Rata-rata NO2                      |
| so2_avg           | FLOAT          | Rata-rata SO2                      |
| o3_avg            | FLOAT          | Rata-rata O3                       |
| uv_index_avg      | FLOAT          | Rata-rata UV Index                 |
| ispu_avg          | FLOAT          | Rata-rata ISPU                     |
| temperature_avg   | FLOAT          | Rata-rata suhu                     |
| humidity_avg      | FLOAT          | Rata-rata kelembaban               |
| wind_speed_avg    | FLOAT          | Rata-rata kecepatan angin          |
| precipitation_sum | FLOAT          | Total curah hujan                  |
| cloud_cover_avg   | FLOAT          | Rata-rata tutupan awan             |
| record_count      | INTEGER        | Jumlah record dalam window         |
| created_at        | TIMESTAMPTZ    | Waktu insert                       |

**Sumber:** Spark Structured Streaming  
**Frekuensi:** Setiap 5 menit  
**Penggunaan:** Grafana dashboard real-time

### 3. `daily_aqi`
Agregasi harian dari batch ETL. Sumber data truth untuk ML training.

| Kolom             | Tipe           | Deskripsi                          |
|-------------------|----------------|-------------------------------------|
| id                | BIGSERIAL (PK) | Auto-increment                     |
| station_id        | VARCHAR(10)    | Kode stasiun                       |
| date              | DATE           | Tanggal (UNIQUE per stasiun)       |
| pm25_avg          | FLOAT          | Rata-rata PM2.5                    |
| pm10_avg          | FLOAT          | Rata-rata PM10                     |
| co_avg            | FLOAT          | Rata-rata CO                       |
| no2_avg           | FLOAT          | Rata-rata NO2                      |
| so2_avg           | FLOAT          | Rata-rata SO2                      |
| o3_avg            | FLOAT          | Rata-rata O3                       |
| uv_index_avg      | FLOAT          | Rata-rata UV Index                 |
| ispu              | FLOAT          | Rata-rata ISPU                     |
| temperature_avg   | FLOAT          | Rata-rata suhu                     |
| humidity_avg      | FLOAT          | Rata-rata kelembaban               |
| wind_speed_avg    | FLOAT          | Rata-rata kecepatan angin          |
| precipitation_sum | FLOAT          | Total curah hujan harian           |
| cloud_cover_avg   | FLOAT          | Rata-rata tutupan awan             |
| aqi_category      | VARCHAR(20)    | Kategori AQI (Baik–Berbahaya)      |
| record_count      | INTEGER        | Jumlah record sumber               |
| created_at        | TIMESTAMPTZ    | Waktu insert                       |

**Constraint:** `UNIQUE(station_id, date)` — upsert-friendly  
**Sumber:** Spark batch ETL (MinIO CSV → clean → aggregate)  
**Penggunaan:** ML training, dashboard harian

### 4. `predictions`
Hasil prediksi ML (dari stream processor & batch predict).

| Kolom           | Tipe           | Deskripsi                          |
|-----------------|----------------|-------------------------------------|
| id              | BIGSERIAL (PK) | Auto-increment                     |
| station_id      | VARCHAR(10)    | Kode stasiun                       |
| window_start    | TIMESTAMPTZ    | Window waktu prediksi              |
| pm25_avg        | FLOAT          | PM2.5 yang diprediksi              |
| pm10_avg        | FLOAT          | PM10 yang diprediksi               |
| predicted_label | VARCHAR(20)    | Label kualitas udara               |
| confidence      | FLOAT          | Confidence score (0–1)             |
| model_version   | VARCHAR(50)    | Versi model ML                     |
| created_at      | TIMESTAMPTZ    | Waktu insert                       |

**Sumber:** Spark Structured Streaming + MLflow model  
**Frekuensi:** Real-time (setiap window 10 menit) + batch harian

### 5. `pipeline_audit`
Log eksekusi pipeline Airflow untuk traceability.

| Kolom       | Tipe           | Deskripsi                          |
|-------------|----------------|-------------------------------------|
| id          | BIGSERIAL (PK) | Auto-increment                     |
| dag_id      | VARCHAR(100)   | ID DAG Airflow                     |
| run_id      | VARCHAR(100)   | Run ID                             |
| status      | VARCHAR(20)    | RUNNING / SUCCESS / FAILED         |
| records_in  | INTEGER        | Jumlah record input                |
| records_out | INTEGER        | Jumlah record output               |
| error_msg   | TEXT           | Pesan error jika gagal             |
| started_at  | TIMESTAMPTZ    | Waktu mulai                        |
| finished_at | TIMESTAMPTZ    | Waktu selesai                      |

**Sumber:** Airflow DAG `batch_ingest` & `ml_retrain`
