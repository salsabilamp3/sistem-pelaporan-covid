import json
from google.cloud import pubsub_v1
import os

credentials_path = r"C:\Users\ASUS\Documents\TINGKAT3\SEMESTER_6\SistemTerdistribusi\TUBES\sistem-pelaporan-covid\credentials.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# Fungsi untuk mengirim pesan ke topik server
def send_message(data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = "projects/sistem-siaga-covid/topics/laporan"
    future = publisher.publish(topic_path, data=data.encode('utf-8'))
    print(f"Sent message: {data}")
    future.result()

# Fungsi untuk menangani pesan respons dari server
def callback(message):
    response = message.data.decode('utf-8')
    print("Received response:", response)
    message.ack()

# Buat subscriber untuk topik respons
subscriber = pubsub_v1.SubscriberClient()
subscription_path = "projects/sistem-siaga-covid/subscriptions/response-sub"
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

# Data contoh laporan
laporan = {}
field_width = 30

laporan['NIK'] = input(f"{'Masukkan NIK':<{field_width}} : ")
laporan['Nama Pelapor'] = input(f"{'Masukkan Nama Pelapor':<{field_width}} : ")
laporan['Nama Terduga Covid'] = input(f"{'Masukkan Nama Terduga Covid':<{field_width}} : ")
laporan['Alamat Terduga Covid'] = input(f"{'Masukkan Alamat Terduga Covid':<{field_width}} : ")
laporan['Gejala'] = input(f"{'Masukkan Gejala':<{field_width}} : ")


# Tulis data laporan ke file JSON
with open('data\data_laporan.json', 'w') as json_file:
    json.dump(laporan, json_file, indent=4)

# Kirim laporan ke server
send_message(';'.join([laporan[key] for key in laporan]))

print("Laporan dikirim")

print(f"Menunggu respons dari server...")
# Agar subscriber tetap berjalan
try:
    with subscriber:
        streaming_pull_future.result()
except TimeoutError:
    streaming_pull_future.cancel()
    streaming_pull_future.result()
except Exception as e:
    print(f"Terjadi kesalahan: {e}")
