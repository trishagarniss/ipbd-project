# Dokumentasi Keamanan Data dan Data Governance

Aspek kemanan data dan tata kelola (_Governance_) merupakan poin vital untuk menjamin integritas dan keamanan pipeline sistem **AQI Watch Surakarta**. Proyek ini menerapkan pendekatan keamanan secara berlapis pada beberapa level layanan.

## 1. Mekanisme Keamanan Data (Security Measures)

Berikut adalah penerapan pengamanan akses dan perlindungan data sensitif pada proyek ini:

### A. Data Protection (Perlindungan Rahasia Sandi)
Seluruh API Keys, token rahasia (seperti Token Telegram untuk Alertmanager), serta password database PostgreSQL dan MinIO disembunyikan menggunakan _Environment Variable_ yang disuntikkan ke dalam berkas `.env`.
File `.env` sudah ditambahkan secara eksplisit ke dalam daftar hitam `.gitignore`, sehingga tidak akan pernah terunggah ke repositori terbuka (GitHub). Saat *deployment*, Docker Compose akan membaca dari file lokal yang dikendalikan penuh oleh Admin (*Secure by Design*).

### B. Authorization (Hak Akses Database Bersyarat)
Sistem ini menggunakan prinsip akses minimum (*principle of least privilege*). Alih-alih memberikan kontrol ke pengguna *superuser* `postgres` atau `root`, proyek menggunakan akun terbatas bernama `aqi_user`. 
Sesuai skrip di file `setup.sql`, `aqi_user` hanya diberikan hak modifikasi (`GRANT ALL PRIVILEGES`) pada tabel spesifik di dalam database `aqi_db`. Jika container atau koneksi Spark berhasil diretas sekalipun, pihak ketiga tidak dapat merusak database atau konfigurasi inti sistem lainnya.

### C. Authentication (Kunci Akses Web UI)
Akses *dashboard* monitoring dan manajemen publik dikunci rapat melalui metode otentikasi login:
- **Apache Airflow**: Seluruh halaman Web UI (DAG monitoring) terkunci; memerlukan otentikasi akun di awal `admin`/`admin123` (atau password kompleks yang dikonfigurasi). 
- **Grafana Dashboard**: Mewajibkan _login_ admin atau viewer dan menyembunyikan konfigurasi koneksi datasource di sisi _backend_, sehingga pengguna tidak dapat meretas query SQL ke dalam *Data Warehouse* PostgreSQL.

---

## 2. Tata Kelola Data (Data Governance)

Data Governance diterapkan agar keakuratan dan kelengkapan data dapat diaudit sewaktu-waktu (Audit Trail) dan memenuhi standar kontrol kualitas sebelum diproses.

### A. Data Quality Monitoring (Validasi Otomatis)
Validasi data dilakukan di beberapa tahap pipeline secara otomatis:
* **Range Validation** di Spark ETL: Memfilter nilai PM2.5, PM10, dan polutan lainnya yang berada di luar rentang wajar atau negatif sebelum disimpan ke Data Warehouse.
* **Null Filtering**: Baris dengan nilai NULL pada kolom kritis (station_id, timestamp) otomatis dibuang.
* **Great Expectations** (standalone): Validasi data quality tambahan dapat dijalankan secara manual menggunakan *Expectation Suite* untuk memeriksa anomali lebih lanjut.
* **Data Quality Check di ML Pipeline**: Script `ml/validation.py` mengecek kelengkapan data (row counts, distinct stations, recency, range validation) dari 4 tabel utama sebelum training.

### B. Audit Trail Pipeline
Setiap kali operasi ETL *Batch* harian berjalan, baik melalui script mandiri (`run_batch.ps1`) maupun Airflow DAG, operasi sisip (insert) log otomatis dikirimkan ke tabel PostgreSQL `pipeline_audit`. Tabel ini mencatat:
- `dag_id` & `run_id`: Identifikasi unik pekerjaan.
- `status`: Keberhasilan atau Kegagalan (*SUCCESS/FAILED*).
- `records_in` & `records_out`: Jumlah selisih baris awal yang dibaca versus jumlah data bersih yang dimasukkan.
- `started_at` & `finished_at`: Stempel waktu (Timestamp).

Berkat *Audit Trail* ini, jika suatu saat terjadi ketidaksesuaian laporan di _Dashboard_, *Data Engineer* dapat dengan mudah melacak balik (*trace-back*) log di tabel ini untuk mendeteksi _bottleneck_ atau kegagalan yang terjadi di masa silam secara transparan.
