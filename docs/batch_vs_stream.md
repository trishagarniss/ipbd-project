# Konsep dan Perbandingan: Batch vs Stream Processing

Proyek **AQI Watch Surakarta** mengimplementasikan dua paradigma utama dalam pengolahan Big Data (Lambda Architecture) untuk menyeimbangkan kebutuhan akan data historis yang sangat besar dan data *real-time* yang cepat.

## 1. Batch Processing
**Batch Processing** adalah metode memproses data dalam bongkahan (volume besar) pada interval waktu yang terjadwal.
Di proyek ini, Batch Processing dikendalikan oleh **Apache Airflow** yang memicu **Apache Spark** (`batch_etl.py`).

### Karakteristik Implementasi di AQI Watch:
*   **Frekuensi Eksekusi:** Terjadwal secara harian (pukul 07:00 WIB / 00:00 UTC).
*   **Volume Data:** Besar. Mengambil data cuaca/polusi agregat 24 jam ke belakang (atau hingga 1 tahun penuh jika dilakukan pengambilan data historis manual).
*   **Sumber Data:** REST API historis (Open-Meteo) yang merespons dengan JSON masif.
*   **Fokus:** Pembersihan data dalam jumlah besar, validasi kualitas secara menyeluruh menggunakan **Great Expectations**, pembuatan model (ML Training), dan penyimpanan arsip jangka panjang ke format `.parquet` di **MinIO Data Lake**.
*   **Kelebihan:** Sangat cocok untuk mengolah jutaan baris data sekaligus secara efisien; mendukung *join* dataset yang sangat kompleks dan pelatihan model ML yang menuntut komputasi berat.

## 2. Stream Processing
**Stream Processing** adalah metode pengolahan data seketika (*real-time* atau *near real-time*) saat data tersebut baru saja dibuat, yang berlangsung terus menerus.
Di proyek ini, aliran data *streaming* ditarik oleh producer ke **Apache Kafka**, kemudian disedot terus menerus oleh **Spark Structured Streaming** (`stream_processor.py`).

### Karakteristik Implementasi di AQI Watch:
*   **Frekuensi Eksekusi:** Terus menerus (berjalan tanpa henti, memproses rekam data seketika ketika tersedia di Kafka).
*   **Volume Data:** Sangat kecil per detiknya (*micro-batch*), kecepatan tinggi (data mengalir tanpa batas/tak terhingga).
*   **Sumber Data:** Simulator Sensor (berperan layaknya perangkat IoT yang memancarkan emisi data per menit atau detik).
*   **Fokus:** Respon yang sangat cepat. Menggunakan sistem *Sliding Window* (10 menit window, 5 menit slide) untuk menghitung rata-rata instan. Segera setelah jendela waktu penuh, data seketika dilempar ke model Machine Learning (MLflow) untuk ditebak kategori polusinya, dan hasilnya langsung di-*push* ke PostgreSQL agar muncul di layar (dashboard Grafana).
*   **Kelebihan:** Menghadirkan pengawasan (monitoring) kualitas udara secara detik-demi-detik sehingga sistem peringatan dini (Alertmanager) dapat mengirim pesan bahaya sebelum krisis/polusi memburuk secara signifikan.

## Kesimpulan

Dalam arsitektur *AQI Watch Surakarta*, kedua pendekatan ini saling melengkapi. **Batch Processing** memastikan keakuratan historis dan fondasi Data Lake yang sehat serta melatih kecerdasan model (ML). Di sisi lain, **Stream Processing** mengonsumsi model cerdas tersebut untuk mendeteksi polusi seketika guna menyajikan _live dashboard_ dan _alerting_ sistem secara reaktif. Pendekatan gabungan ini adalah pilar utama dari arsitektur Big Data modern (Lambda Architecture).
