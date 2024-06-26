from datetime import datetime
import json
import uuid
import os
from dotenv import load_dotenv
from google.cloud import pubsub_v1
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
        future.result()
    except Exception as e:
        print(f"Terjadi kesalahan saat mengirim pesan: {e}")

# Fungsi untuk memformat tanggal dan waktu
def format_datetime(datetime_str):
    dt = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    return dt.strftime("%d %B %Y, %H:%M:%S")

# Fungsi untuk mencetak respons
def print_response(id_laporan, waktu_penjemputan, nama_penjemput, jumlah_orang):
    print("\n======= Respons dari Server =======")
    print(f"IDLaporan        : {id_laporan}")
    print(f"Waktu Penjemputan: {waktu_penjemputan}")
    print(f"Nama Penjemput   : {nama_penjemput}")
    print(f"Jumlah Penjemput : {jumlah_orang}")
    print("===================================")

# Fungsi untuk mencetak pesan kesalahan
def print_error_message(id_laporan, error_message):
    print("\n=== Pesan Kesalahan dari Server ===")
    print(f"IDLaporan        : {id_laporan}")
    print(f"Pesan Kesalahan  : {error_message}")
    print("===================================")

# Fungsi untuk menangani pesan respons dari server
def callback(message):
    try:
        response = message.data.decode('utf-8')
        id_laporan, respon_detail = response.split(';', 1)
        
        # Periksa apakah respon_detail mengandung informasi penjemputan atau pesan kesalahan
        if "Waktu Penjemputan" in respon_detail:
            detail_items = respon_detail.split(',')
            waktu_penjemputan = format_datetime(detail_items[0].split(': ')[1])
            nama_penjemput = detail_items[1].split(': ')[1]
            jumlah_orang = detail_items[2].split(': ')[1]
            print_response(id_laporan, waktu_penjemputan, nama_penjemput, jumlah_orang)
        else:
            print_error_message(id_laporan, respon_detail)
            
        message.ack()
        
        # Tanyakan pengguna apakah ingin mengirim laporan lagi
        send_another = input("\nApakah Anda ingin mengirim laporan lagi? (y/n): ").lower()
        if send_another == 'y':
            create_and_send_report()
        else:
            print("Terima kasih.")
            os._exit(0)
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

def create_and_send_report():
    laporan = {}
    field_width = 22

    try:
        # Buat struktur data laporan yang baru
        laporan['IDLaporan'] = str(uuid.uuid4())
        print("======== Informasi Pelapor ========")
        laporan['NIK'] = input(f"{'Masukkan NIK':<{field_width}} : ")
        laporan['Nama Pelapor'] = input(f"{'Masukkan Nama Pelapor':<{field_width}} : ")
        laporan['Pasien'] = []
        jumlah_pasien = int(input("Masukkan jumlah pasien yang akan dilaporkan : "))

        print("\n======== Informasi Pasien =========")
        for i in range(jumlah_pasien):
            pasien = {}
            print(f"{'----------- Pasien ke ' + str(i+1)} -----------")
            pasien['Nama Terduga Covid'] = input(f"{'Nama Terduga Covid':<{field_width}} : ")
            pasien['Alamat Terduga Covid'] = input(f"{'Alamat Terduga Covid':<{field_width}} : ")
            pasien['Gejala'] = input(f"{'Gejala':<{field_width}} : ")
            laporan['Pasien'].append(pasien)
        print("===================================")

        # Sisipkan informasi penjemputan setelah informasi pasien
        laporan['Penjemputan'] = {
            "ID_Penjemput": "",
            "Nama_Lengkap": "",
            "Nomor_Telepon": "",
            "Nomor_Kendaraan": "",
            "Waktu_Penjemputan": ""
        }

        # Menambahkan laporan ke dalam data laporan yang ada
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
        send_message(json.dumps(laporan))

        print("\n--------- Laporan dikirim ---------")

        print(f"\n-- Menunggu respons dari server ---")

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

# Memulai proses pengiriman laporan
create_and_send_report()