from google.cloud import pubsub_v1
import time

# Fungsi untuk menangani pesan yang diterima dari topik
def callback(message):
    # Lakukan validasi data pesan (contoh sederhana)
    data = message.data.decode('utf-8').split(',')
    if len(data) != 5:
        print("Invalid message format")
        message.ack()
        return

    # Lakukan validasi NIK dengan database (contoh sederhana)
    nik = data[0]
    if nik not in database:
        print("Invalid NIK")
        message.ack()
        return

    # Respon kepada client dengan informasi waktu dan jumlah orang penjemputan
    waktu = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    nama = data[1]
    jumlah_orang = 1  # Misalnya hanya 1 orang penjemputan
    respon = f"{waktu}, {nama}, Jumlah Orang Penjemputan: {jumlah_orang}"
    print(f"Respon: {respon}")

    # Kirim respons ke client menggunakan message queue
    send_response(respon)
    message.ack()

# Fungsi untuk mengirim respons ke client menggunakan message queue
def send_response(respon):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('your-project-id', 'client-response-topic')
    future = publisher.publish(topic_path, data=respon.encode('utf-8'))
    print(f"Sent response: {respon}")
    future.result()

# Nama topik yang digunakan
topic_name = 'projects/your-project-id/topics/server-topic'

# Buat subscriber
subscriber = pubsub_v1.SubscriberClient()

# Subscribe ke topik
subscription_path = subscriber.subscription_path('your-project-id', 'server-subscription')
subscriber.subscribe(subscription_path, callback=callback)

print(f"Menunggu pesan dari topik {topic_name}...")
while True:
    time.sleep(5)  # Jaga agar program tetap berjalan