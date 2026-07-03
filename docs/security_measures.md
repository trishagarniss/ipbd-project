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
Kami secara ketat menggunakan alat standar industri, **Great Expectations**, di dalam alur *Airflow DAG* kami (`dags/batch_ingest.py`). Sebelum pipeline ETL Spark menyimpan rekam data ke dalam Data Warehouse, _Expectation Suite_ akan melakukan pemindaian otomatis, contohnya:
* Memastikan nilai Kolom (seperti PM2.5 dan PM10) tidak boleh `Null` atau negatif.
* Memastikan bahwa partikel polusi tidak melebihi rentang wajar atmosferik.
Jika kondisi/ekspektasi ini gagal, pipeline akan menghentikan alur (*failing the task*) sehingga data kotor tidak mencemari model Machine Learning maupun hasil akhir laporan (*Dashboard*).

### B. Audit Trail Pipeline
Setiap kali operasi ETL *Batch* harian berjalan, baik melalui script mandiri (`run_batch.ps1`) maupun Airflow DAG, operasi sisip (insert) log otomatis dikirimkan ke tabel PostgreSQL `pipeline_audit`. Tabel ini mencatat:
- `dag_id` & `run_id`: Identifikasi unik pekerjaan.
- `status`: Keberhasilan atau Kegagalan (*SUCCESS/FAILED*).
- `records_in` & `records_out`: Jumlah selisih baris awal yang dibaca versus jumlah data bersih yang dimasukkan.
- `started_at` & `finished_at`: Stempel waktu (Timestamp).

Berkat *Audit Trail* ini, jika suatu saat terjadi ketidaksesuaian laporan di _Dashboard_, *Data Engineer* dapat dengan mudah melacak balik (*trace-back*) log di tabel ini untuk mendeteksi _bottleneck_ atau kegagalan yang terjadi di masa silam secara transparan.
