from kafka import KafkaConsumer
from minio import Minio
import json
import pandas as pd
import time
from datetime import datetime
import io

while True:
    try:
        consumer = KafkaConsumer(
            'jobs-topic',
            bootstrap_servers=['kafka:29092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        print("✅ Connected to Kafka. Waiting for messages...")
        break
    except Exception as e:
        print(f"❌ Kafka connection error: {e}")
        time.sleep(5)

# MinIO Client
minio_client = Minio(
    "minio:9000",
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False
)

bucket_name = "jobs"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# Batch buffer
buffer = []
last_save = time.time()

while True:
    for msg in consumer:
        buffer.append(msg.value)
        now = time.time()
        print(f"[📥] Received job: {msg.value.get('Job Title', 'Unknown')}")

        if now - last_save >= 120:  # Save every 2 minutes
            if buffer:
                df = pd.DataFrame(buffer)
                filename = f"jobs_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

                csv_buf = io.StringIO()
                df.to_csv(csv_buf, index=False)

                minio_client.put_object(
                    bucket_name,
                    filename,
                    io.BytesIO(csv_buf.getvalue().encode()),
                    length=len(csv_buf.getvalue()),
                    content_type='application/csv'
                )

                print(f"[✅] Saved {len(buffer)} jobs to MinIO as '{filename}'")
                buffer.clear()
                last_save = now
