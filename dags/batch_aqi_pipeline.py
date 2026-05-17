from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import great_expectations as gx

# Sesuaikan DB_URI dengan username dan password di file .env milikmu
# Format: postgresql://<user>:<password>@<nama_service_db>:5432/<nama_db>
DB_URI = "postgresql://myuser:mypassword@postgres:5432/aqi_jakarta"
FILE_PATH = "/opt/airflow/data/ispu_jakarta_historis.csv"

@dag(
    dag_id="batch_ingest_aqi_jakarta",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["aqi", "batch", "tbp"],
)
def batch_pipeline():

    @task
    def run_great_expectations():
        """Validasi data CSV menggunakan aturan (Suite) yang sudah kita buat."""
        print("Memulai proses validasi data...")
        
        # 1. Panggil context dari folder gx yang sudah kita mount
        context = gx.get_context(mode="file", project_root_dir="/opt/airflow")
        
        # 2. Panggil aturan (Suite) bernama 'aqi_suite'
        suite = context.suites.get(name="aqi_suite")
        
        # 3. Beri tahu Great Expectations letak file CSV kita
        data_source = context.data_sources.add_pandas("csv_data_source")
        data_asset = data_source.add_csv_asset("ispu_data_asset", filepath_or_buffer=FILE_PATH)
        batch_def = data_asset.add_batch_definition_whole_dataframe("whole_file")
        
        # 4. Gabungkan data dan aturan, lalu jalankan
        validation_def = gx.ValidationDefinition(
            data=batch_def,
            suite=suite,
            name="aqi_validation"
        )
        
        results = validation_def.run()
        
        # 5. Cek hasilnya
        if not results.success:
            raise ValueError("🚨 Validasi GAGAL! Ada data kosong atau PM2.5 di luar batas 0-500.")
        
        print("✅ Validasi SUKSES! Data aman dan sesuai aturan.")
        return FILE_PATH

    @task
    def load_to_postgres(file_path):
        """Memasukkan data ke database setelah lolos sensor (validasi)."""
        print(f"Membaca data bersih dari {file_path}")
        df = pd.read_csv(file_path)
        
        # Buka koneksi ke database
        engine = create_engine(DB_URI)
        
        # Load data ke tabel raw_measurements
        # if_exists='append' memastikan data baru ditambah ke baris bawah, bukan menimpa yang lama
        df.to_sql('raw_measurements', engine, if_exists='append', index=False)
        print(f"🎉 Berhasil memuat {len(df)} baris data ke PostgreSQL!")

    # Mendefinisikan urutan eksekusi tugas (Dependencies)
    # Tugas load hanya akan berjalan jika tugas validasi berhasil (return file_path)
    data_path = run_great_expectations()
    load_to_postgres(data_path)

# Mendaftarkan DAG
dag_instance = batch_pipeline()