import json
from google.cloud import pubsub_v1
import time
import os
from concurrent.futures import TimeoutError

credentials_path = r"D:\SISTER\sistem-pelaporan-covid\credentials.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# Baca data kependudukan dari file JSON
with open("data\data_kependudukan.json") as f:
    data_kependudukan = json.load(f)

# Fungsi untuk menangani pesan yang diterima dari topik
def callback(message):
    # Lakukan validasi data pesan (contoh sederhana)
    data = message.data.decode('utf-8').split(';')
    print(data)
    if len(data) != 5:
        print("Invalid message format")
        message.ack()
        return

    # Dapatkan NIK dan nama dari data pesan
    nik = data[0]
    nama = data[1]

    # Lakukan validasi NIK dan nama dengan data kependudukan dari file JSON
    if not validate_nik(nik, nama):
        print("Invalid NIK or name")
        message.ack()
        return

    # Respon kepada client dengan informasi waktu dan jumlah orang penjemputan
    waktu = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    jumlah_orang = 1  # Misalnya hanya 1 orang penjemputan
    respon = f"{waktu}, {nama}, Jumlah Orang Penjemputan: {jumlah_orang}"
    print(f"Respon: {respon}")

    # Kirim respons ke client menggunakan message queue
    send_response(respon)
    message.ack()

# Fungsi untuk validasi NIK dan nama berdasarkan data kependudukan
def validate_nik(nik, nama):
    for person in data_kependudukan:
        if person["NIK"] == nik and person["Nama"] == nama:
            return True
    return False

# Fungsi untuk mengirim respons ke client menggunakan message queue
def send_response(respon):
    publisher = pubsub_v1.PublisherClient()
    topic_path = "projects/sistem-siaga-covid/topics/response"
    future = publisher.publish(topic_path, data=respon.encode('utf-8'))
    print(f"Sent response: {respon}")
    future.result()

# Buat subscriber
subscriber = pubsub_v1.SubscriberClient()

# Subscribe ke topik
subscription_path = "projects/sistem-siaga-covid/subscriptions/laporan-sub"
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print(f"Menunggu pesan dari topik...")
try:
    with subscriber:
        try:
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
except Exception as e:
    print(f"Terjadi kesalahan: {e}")
    time.sleep(5)  # Tunggu sebentar sebelum mencoba lagi
