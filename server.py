import json
from google.cloud import pubsub_v1
import time
import os
from dotenv import load_dotenv
from concurrent.futures import TimeoutError

load_dotenv()

credentials_path = os.getenv('CREDENTIALS_PATH')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# Baca data kependudukan dari file JSON
with open(r"data\data_kependudukan.json") as f:
    data_kependudukan = json.load(f)

# Baca data penjemput dari file JSON
with open(r"data\data_laporan.json") as f:
    data_laporan = json.load(f)

# Baca data penjemput dari file JSON
with open(r"data\data_informasi_penjemput.json") as f:
    data_penjemput = json.load(f)

# Fungsi untuk menangani pesan yang diterima dari topik
def callback(message):
    # Lakukan validasi data pesan (contoh sederhana)
    data = message.data.decode('utf-8').split(';')
    print(data)
    if len(data) != 6:
        print("Invalid message format")
        message.ack()
        return

    # Dapatkan IDLaporan, NIK, dan nama dari data pesan
    id_laporan = data[0]
    nik = data[1]
    nama = data[2]

    # Lakukan validasi NIK dan nama dengan data kependudukan dari file JSON
    valid_nik, valid_nama, reason = validate_nik(nik, nama)
    if valid_nik and valid_nama:
        # Pilih penjemput dengan last_schedule terkecil
        selected_penjemput = min(data_penjemput, key=lambda x: x.get('last_schedule', float('inf')))
        
        # Respon kepada client dengan informasi waktu penjemputan, nama penjemput, dan jumlah orang penjemputan
        waktu_penjemputan = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        nama_penjemput = selected_penjemput["Nama_Lengkap"]
        jumlah_orang = 1  # Misalnya hanya 1 orang penjemputan

        # Perbarui last_schedule penjemput yang dipilih
        last_schedule = selected_penjemput.get("last_schedule")
        if last_schedule and last_schedule >= waktu_penjemputan:
            # Jika last_schedule belum terlewati, atur waktu penjemputan 10 menit setelah last_schedule
            new_schedule = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.mktime(time.strptime(last_schedule, "%Y-%m-%d %H:%M:%S")) + 600))
            selected_penjemput["last_schedule"] = new_schedule
        else:
            # Jika last_schedule sudah terlewati atau belum diatur, atur waktu penjemputan sama dengan waktu sekarang
            selected_penjemput["last_schedule"] = waktu_penjemputan

        # Tambahkan object penjemputan ke dalam data laporan
        for penjemput in data_penjemput:
            if penjemput["ID_Penjemput"] == selected_penjemput["ID_Penjemput"]:
                penjemput["last_schedule"] = selected_penjemput["last_schedule"]
                break
        
        # Tulis data laporan ke file JSON
        with open("data\data_informasi_penjemput.json", 'w') as json_file:
            json.dump(data_penjemput, json_file, indent=4)

        waktu_penjemputan = selected_penjemput["last_schedule"]
        
        # Tulis data laporan yang sudah diperbarui ke file JSON
        with open("data\data_laporan.json", 'r+') as json_file:
            data_laporan = json.load(json_file)
            for laporan in data_laporan:
                if laporan["IDLaporan"] == id_laporan:
                    laporan["Penjemputan"] = {
                        "ID_Penjemput": selected_penjemput["ID_Penjemput"],
                        "Nama_Lengkap": selected_penjemput["Nama_Lengkap"],
                        "Nomor_Telepon": selected_penjemput["Nomor_Telepon"],
                        "Nomor_Kendaraan": selected_penjemput["Nomor_Kendaraan"],
                        "Waktu_Penjemputan": waktu_penjemputan
                    }
                    break
            json_file.seek(0)  # Pindah ke awal file
            json.dump(data_laporan, json_file, indent=4)
            json_file.truncate()  # Potong ke panjang yang benar jika perlu
        
        respon_detail = f"Waktu Penjemputan: {waktu_penjemputan}, Nama Penjemput: {nama_penjemput}, Jumlah Orang Penjemputan: {jumlah_orang}"
        
    else:
        respon_detail = f"Kesalahan: {reason}"

    respon = f"{id_laporan};{respon_detail}"
    print(f"Respon: {respon}")

    # Kirim respons ke client menggunakan message queue
    send_response(respon)
    message.ack()
# Fungsi untuk validasi NIK dan nama berdasarkan data kependudukan
def validate_nik(nik, nama):
    valid_nik = False
    valid_nama = False
    reason = ""
    for person in data_kependudukan:
        if person["NIK"] == nik:
            valid_nik = True
            if person["Nama"] == nama:
                valid_nama = True
                break
            else:
                reason = f"NIK {nik} terdaftar dengan nama {person['Nama']}, bukan {nama}"
                break
    if not valid_nik:
        for person in data_kependudukan:
            if person["Nama"] == nama:
                reason = f"Nama {nama} terdaftar dengan NIK {person['NIK']}, bukan {nik}"
                break
        if not reason:
            reason = f"NIK {nik} tidak ditemukan"
    return valid_nik, valid_nama, reason

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
