# Dashboard Grafana — Tujuan & Insight

## Tujuan Dashboard

Dashboard ini dirancang untuk tiga peran utama:

### 1. **Masyarakat Umum**
- Melihat kualitas udara terkini di 5 kecamatan Surakarta
- Mendapatkan rekomendasi aktivitas (aman/tidak aman di luar ruangan)
- Histori tren harian/mingguan/bulanan

### 2. **Dinas Lingkungan Hidup**
- Monitoring real-time polusi udara
- Deteksi dini lonjakan polutan (alert via Telegram)
- Data pendukung keputusan (himbauan work-from-home, pembatasan outdoor)

### 3. **Data Scientist / Peneliti**
- Dataset harian untuk analisis korelasi polutan vs cuaca
- Prediksi kualitas udara (model ML) yang bisa divalidasi
- Perbandingan antar stasiun (spatial analysis)

## Insight yang Bisa Diambil

| Insight | Sumber Data | Visualisasi |
|---------|------------|-------------|
| Tren AQI harian per stasiun | `stream_agg` / `daily_aqi` | Time series line chart |
| Pola mingguan (weekend vs weekday) | `daily_aqi` | Bar chart + t test |
| Stasiun dengan polusi tertinggi | `daily_aqi` | Bar chart ranking |
| Korelasi PM2.5 vs cuaca | `daily_aqi` | Scatter plot |
| Heatmap jam sibuk vs AQI | `stream_agg` | Heatmap (jam × hari) |
| Prediksi vs aktual | `predictions` + `stream_agg` | Dual line chart |
| Distribusi kategori AQI | `daily_aqi` | Pie / donut chart |
| Peta persebaran AQI kota | `daily_aqi` | Geomap (bubble) |
| Cluster stasiun (KMeans) | `daily_aqi` | Scatter (PCA) |

## Panel yang Direkomendasikan

### Row 1: Ringkasan
- **Stat:** Rata-rata AQI Surakarta (hari ini)
- **Stat:** Stasiun dengan AQI terburuk
- **Stat:** Jumlah stasiun dalam kategori "Tidak Sehat"
- **Stat:** Persentase data coverage (24 jam)

### Row 2: Time Series
- **Line chart:** European AQI 5 stasiun (7 hari)
- **Line chart:** PM2.5 + PM10 5 stasiun (24 jam)
- **Line chart:** Suhu + Kelembaban (7 hari)

### Row 3: Analisis Harian
- **Bar chart:** Rata-rata harian PM2.5 per stasiun
- **Bar chart:** Kategori AQI distribution (stacked)
- **Heatmap:** Rata-rata AQI per jam × hari

### Row 4: Prediksi & Geospasial
- **Line chart:** Prediksi vs aktual AQI
- **Table:** Prediksi terbaru + confidence
- **Geomap:** Peta Surakarta dengan bubble AQI

## Alert Rules (Prometheus)

| Rule | Threshold | Severity | Kanal |
|------|-----------|----------|-------|
| AQI Kritis | european_aqi_avg > 80 (30 menit) | critical | Telegram |
| AQI Warning | european_aqi_avg > 60 (1 jam) | warning | Telegram |
| PM2.5 Tinggi | pm25_avg > 50 (1 jam) | warning | Telegram |
| Kafka Down | up == 0 (2 menit) | critical | Telegram |
| PostgreSQL Down | up == 0 (2 menit) | critical | Telegram |

## Batasan & Asumsi

- **Data:** Menggunakan Open-Meteo API (model, bukan sensor fisik) — akurasi ±15%
- **Cakupan:** 5 titik kecamatan — interpolasi linier untuk area antar stasiun
- **Ground truth:** European AQI dari API — tidak divalidasi dengan ISPU manual
- **Prediksi:** Random Forest — confidence 0.85 (baseline, perlu tuning lanjutan)
