from kafka import KafkaConsumer
import csv
import os

# Inisialisasi Kafka consumer
consumer = KafkaConsumer(
    'netflix-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='netflix-consumer-group'
)

# Folder untuk menyimpan file batch
output_folder = 'batched_data'
os.makedirs(output_folder, exist_ok=True)

# Pengaturan batch
batch_size = 100  # Jumlah data per batch
batch = []
batch_number = 1

def save_batch(data, batch_num):
    filename = f'{output_folder}/batch_{batch_num}.csv'
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)
    print(f'Saved {filename}')

# Membaca data dari Kafka dan menyimpan dalam batch
for message in consumer:
    row = message.value.decode('utf-8').split(',')
    batch.append(row)
    
    # Jika batch sudah mencapai ukuran yang diinginkan, simpan ke file
    if len(batch) >= batch_size:
        save_batch(batch, batch_number)
        batch = []
        batch_number += 1
