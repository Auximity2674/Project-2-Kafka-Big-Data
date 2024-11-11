import csv
import time
import random
from kafka import KafkaProducer

# Inisialisasi Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Nama file dataset Netflix
file_path = '/mnt/data/data.csv'

# Membaca file CSV dan mengirim data ke Kafka topic
def send_data():
    with open(file_path, mode='r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row jika ada
        for row in reader:
            # Convert row data to a comma-separated string
            message = ','.join(row)
            # Kirim data ke Kafka topic "netflix-data"
            producer.send('netflix-data', value=message.encode('utf-8'))
            print(f'Sent: {message}')
            # Jeda acak untuk mensimulasikan data streaming
            time.sleep(random.uniform(0.5, 2))

if __name__ == "__main__":
    send_data()
    producer.close()
