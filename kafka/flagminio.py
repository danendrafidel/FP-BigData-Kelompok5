import pandas as pd
import requests
from minio import Minio
import os
import io

# Inisialisasi MinIO client
minio_client = Minio(
    "localhost:9000",  # Ganti jika MinIO berjalan di host/port lain
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False
)

bucket_name = "flags"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# Baca CSV dengan path absolut berbasis lokasi script
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, "flags.csv")
df = pd.read_csv(csv_path, sep=';')

for idx, row in df.iterrows():
    name = row['name']
    url = row['image']
    if not isinstance(url, str) or not url.startswith("http"):
        print(f"Skip {name}: URL tidak valid atau kosong")
        continue

    # Ekstensi file dari URL
    ext = os.path.splitext(url)[-1]
    if ext not in [".svg", ".png", ".jpg", ".jpeg"]:
        ext = ".png"  # fallback

    filename = f"{name.replace(' ', '_')}{ext}".lower()

    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            print(f"✗ Failed {name}: HTTP {resp.status_code}")
            continue
        data = resp.content

        minio_client.put_object(
            bucket_name,
            filename,
            data=io.BytesIO(data),
            length=len(data),
            content_type="image/svg+xml" if ext == ".svg" else "image/png"
        )
        print(f"✓ Uploaded {filename} to MinIO")
    except Exception as e:
        print(f"✗ Failed {name}: {e}")