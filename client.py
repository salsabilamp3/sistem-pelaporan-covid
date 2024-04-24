from google.cloud import pubsub_v1

# Fungsi untuk mengirim pesan ke topik server
def send_message(data):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path('your-project-id', 'server-topic')
    future = publisher.publish(topic_path, data=data.encode('utf-8'))
    print(f"Sent message: {data}")
    future.result()

# Fungsi untuk menangani pesan respons dari server
def callback(message):
    response = message.data.decode('utf-8')
    print("Received response:", response)
    message.ack()

# Nama topik yang digunakan untuk respons dari server
response_topic_name = 'projects/your-project-id/topics/client-response-topic'

# Buat subscriber untuk topik respons
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('your-project-id', 'client-subscription')
subscriber.subscribe(subscription_path, callback=callback)

# Data contoh laporan
laporan = {
    "NIK": "1234567890",
    "Nama Pelapor": "John Doe",
    "Nama Terduga Covid": "Jane Doe",
    "Alamat Terduga Covid": "Jalan Contoh No. 123",
    "Gejala": "Demam, batuk"
}

# Kirim laporan ke server
send_message(','.join([laporan[key] for key in laporan]))

print("Laporan dikirim")

print(f"Menunggu respons dari server...")
while True:
    pass  # Tidak perlu melakukan apa pun selain menunggu pesan respons
