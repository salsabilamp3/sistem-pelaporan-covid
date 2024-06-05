import json
import uuid
from google.cloud import pubsub_v1
import os
from dotenv import load_dotenv
from concurrent.futures import TimeoutError

# Memuat variabel lingkungan dari file .env
load_dotenv()

credentials_path = os.getenv('CREDENTIALS_PATH')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# Fungsi untuk mengirim pesan ke topik server
def send_message(data):
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = "projects/sistem-siaga-covid/topics/laporan"
        future = publisher.publish(topic_path, data=data.encode('utf-8'))
        print(f"Sent message: {data}")
        future.result()
    except Exception as e:
        print(f"Terjadi kesalahan saat mengirim pesan: {e}")

# Fungsi untuk menangani pesan respons dari server
def callback(message):
    try:
        response = message.data.decode('utf-8')
        id_laporan, respon_detail = response.split(';', 1)
        print(f"Received response for IDLaporan {id_laporan}: {respon_detail}")
        message.ack()
    except Exception as e:
        print(f"Terjadi kesalahan saat menangani respons dari server: {e}")

# Fungsi untuk membuat subscriber
def create_subscriber():
    try:
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = "projects/sistem-siaga-covid/subscriptions/response-sub"
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        return subscriber, streaming_pull_future
    except Exception as e:
        print(f"Terjadi kesalahan saat membuat subscriber: {e}")
        return None, None

data_laporan_path = r'data\data_laporan.json'

laporan = {}
field_width = 30

try:
    laporan['IDLaporan'] = str(uuid.uuid4())
    laporan['NIK'] = input(f"{'Masukkan NIK':<{field_width}} : ")
    laporan['Nama Pelapor'] = input(f"{'Masukkan Nama Pelapor':<{field_width}} : ")
    laporan['Nama Terduga Covid'] = input(f"{'Masukkan Nama Terduga Covid':<{field_width}} : ")
    laporan['Alamat Terduga Covid'] = input(f"{'Masukkan Alamat Terduga Covid':<{field_width}} : ")
    laporan['Gejala'] = input(f"{'Masukkan Gejala':<{field_width}} : ")
    laporan['Penjemputan'] = {
            "ID_Penjemput": "",
            "Nama_Lengkap": "",
            "Nomor_Telepon": "",
            "Nomor_Kendaraan": "",
            "Waktu_Penjemputan": ""
    }  

    if os.path.exists(data_laporan_path):
        with open(data_laporan_path, 'r') as json_file:
            data_laporan = json.load(json_file)
    else:
        data_laporan = []
    
    data_laporan.append(laporan)

    # Tulis data laporan ke file JSON
    with open(data_laporan_path, 'w') as json_file:
        json.dump(data_laporan, json_file, indent=4)

    # Kirim laporan ke server
    send_message(';'.join([laporan[key] for key in list(laporan.keys())[:-1]]))

    print("Laporan dikirim")

    print(f"Menunggu respons dari server...")
    # Agar subscriber tetap berjalan
    subscriber, streaming_pull_future = create_subscriber()
    if subscriber:
        try:
            with subscriber:
                streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
        except Exception as e:
            print(f"Terjadi kesalahan: {e}")
except Exception as e:
    print(f"Terjadi kesalahan saat memproses laporan: {e}")
