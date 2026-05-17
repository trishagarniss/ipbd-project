import great_expectations as gx
import great_expectations.expectations as gxe

print("Memulai inisialisasi Great Expectations (V1+)...")

# 1. Inisialisasi Context
# mode="file" akan otomatis membuat folder 'gx' beserta isinya di dalam direktori proyekmu
context = gx.get_context(mode="file", project_root_dir=".")
print("✅ Folder 'gx' berhasil diinisialisasi!")

# 2. Membuat kumpulan aturan (Expectation Suite)
suite_name = "aqi_suite"
suite = gx.ExpectationSuite(name=suite_name)

print("Memasukkan aturan-aturan validasi...")

# Aturan 1: Kolom 'value' dan 'measured_at' tidak boleh kosong (Null Check)
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="value"))
suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="measured_at"))

# Aturan 2: Validasi range PM2.5 di angka 0 - 500
suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="value", min_value=0, max_value=500))

# Aturan 3: Timestamp ordering / format check
suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column="measured_at", type_="str"))

# 4. Menyimpan Suite ke dalam folder 'gx'
context.suites.add_or_update(suite)
print(f"✅ Expectation Suite '{suite_name}' berhasil disimpan dan siap digunakan oleh Airflow!")