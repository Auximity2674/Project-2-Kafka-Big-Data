import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inisialisasi Spark session
spark = SparkSession.builder \
    .appName("Netflix Spark Consumer") \
    .getOrCreate()

# Folder yang berisi file batch dari Kafka consumer
batch_folder = 'batched_data'
processed_folder = 'processed_batches'

# Membuat folder untuk menyimpan hasil pemrosesan (jika belum ada)
os.makedirs(processed_folder, exist_ok=True)

# Fungsi untuk memproses dan melatih model dengan data batch
def process_and_train_model(batch_file, model_number):
    # Membaca file batch sebagai DataFrame Spark
    df = spark.read.csv(batch_file, header=False, inferSchema=True)
    
    # Menambahkan nama kolom (sesuaikan dengan kolom dataset Netflix)
    df = df.toDF("show_id", "type", "title", "director", "cast", "country", "date_added", 
                 "release_year", "rating", "duration", "listed_in", "description")
    
    # Contoh pemrosesan: hanya memilih kolom `type`, `title`, dan `release_year`
    df = df.select("type", "title", "release_year")
    
    # Tampilkan 5 data pertama (opsional)
    df.show(5)
    
    # Simpan hasil pemrosesan (opsional)
    processed_file = f"{processed_folder}/processed_batch_{model_number}.csv"
    df.write.csv(processed_file, header=True)
    print(f"Processed data saved to {processed_file}")

    # Implementasi model sederhana (contoh clustering atau filter tertentu)
    # Anda dapat menambahkan model training yang lebih kompleks sesuai kebutuhan
    
    # Contoh dummy model output
    print(f"Model {model_number} trained with data from {batch_file}")

# Membaca dan memproses setiap file batch
batch_files = sorted([os.path.join(batch_folder, f) for f in os.listdir(batch_folder) if f.endswith('.csv')])

# Menjalankan pemrosesan batch
for i, batch_file in enumerate(batch_files, start=1):
    process_and_train_model(batch_file, model_number=i)
