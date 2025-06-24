import pandas as pd
import requests
from minio import Minio
import os
import io
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

# Setup session dengan retry mechanism yang lebih cepat
session = requests.Session()
retry_strategy = Retry(
    total=1,  # Hanya 1 retry
    backoff_factor=0.1,  # Delay minimal
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

# Baca CSV dengan path absolut berbasis lokasi script
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, "flags.csv")
df = pd.read_csv(csv_path)

success_count = 0
failed_count = 0
total_count = len(df)

print(f"Mulai upload {total_count} flags ke MinIO...")
start_time = time.time()

# Fungsi untuk membuat placeholder flag sederhana
def create_placeholder_flag(country_code):
    """Membuat placeholder flag sederhana dengan SVG"""
    svg_content = f'''<?xml version="1.0" encoding="UTF-8"?>
<svg width="40" height="30" xmlns="http://www.w3.org/2000/svg">
  <rect width="40" height="30" fill="#f0f0f0" stroke="#ccc" stroke-width="1"/>
  <text x="20" y="18" font-family="Arial" font-size="8" text-anchor="middle" fill="#666">{country_code.upper()}</text>
</svg>'''
    return svg_content.encode('utf-8')

for idx, row in df.iterrows():
    country = row['Country']
    country_code = row['Country code']
    flag_url = row['Flag']
    
    # Progress indicator
    progress = (idx + 1) / total_count * 100
    print(f"[{progress:.1f}%] Processing {country}...", end=" ")
    
    if not isinstance(flag_url, str) or not flag_url.startswith("http"):
        print("SKIP - URL tidak valid")
        failed_count += 1
        continue

    # Ekstensi file dari URL
    ext = os.path.splitext(flag_url)[-1]
    if ext not in [".svg", ".png", ".jpg", ".jpeg"]:
        ext = ".png"  # fallback

    # Gunakan country code sebagai filename untuk konsistensi
    filename = f"{country_code.lower()}{ext}"

    try:
        # Coba download dengan timeout yang sangat pendek
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        resp = session.get(flag_url, timeout=5, headers=headers)  # Timeout sangat pendek
        if resp.status_code != 200:
            print(f"FAILED - HTTP {resp.status_code}, creating placeholder")
            # Buat placeholder flag
            data = create_placeholder_flag(country_code)
            content_type = "image/svg+xml"
            filename = f"{country_code.lower()}.svg"
        else:
            data = resp.content
            
            # Validasi minimal
            if len(data) < 50:  # Kurangi threshold
                print(f"FAILED - File terlalu kecil, creating placeholder")
                data = create_placeholder_flag(country_code)
                content_type = "image/svg+xml"
                filename = f"{country_code.lower()}.svg"
            else:
                # Tentukan content type berdasarkan ekstensi
                content_type = "image/png"  # default
                if ext == ".svg":
                    content_type = "image/svg+xml"
                elif ext in [".jpg", ".jpeg"]:
                    content_type = "image/jpeg"

        # Upload ke MinIO
        minio_client.put_object(
            bucket_name,
            filename,
            data=io.BytesIO(data),
            length=len(data),
            content_type=content_type
        )
        print(f"SUCCESS - {len(data)} bytes")
        success_count += 1
        
    except requests.exceptions.Timeout:
        print("FAILED - Timeout, creating placeholder")
        # Buat placeholder flag untuk timeout
        try:
            data = create_placeholder_flag(country_code)
            filename = f"{country_code.lower()}.svg"
            minio_client.put_object(
                bucket_name,
                filename,
                data=io.BytesIO(data),
                length=len(data),
                content_type="image/svg+xml"
            )
            print(f"SUCCESS - Placeholder created")
            success_count += 1
        except Exception as e:
            print(f"FAILED - Cannot create placeholder: {e}")
            failed_count += 1
    except requests.exceptions.ConnectionError as e:
        print("FAILED - Connection error, creating placeholder")
        # Buat placeholder flag untuk connection error
        try:
            data = create_placeholder_flag(country_code)
            filename = f"{country_code.lower()}.svg"
            minio_client.put_object(
                bucket_name,
                filename,
                data=io.BytesIO(data),
                length=len(data),
                content_type="image/svg+xml"
            )
            print(f"SUCCESS - Placeholder created")
            success_count += 1
        except Exception as e:
            print(f"FAILED - Cannot create placeholder: {e}")
            failed_count += 1
    except Exception as e:
        print(f"FAILED - {str(e)[:50]}...")
        failed_count += 1

end_time = time.time()
duration = end_time - start_time

print(f"\n=== Upload Summary ===")
print(f"✓ Successfully uploaded: {success_count} flags")
print(f"✗ Failed uploads: {failed_count} flags")
print(f"Total processed: {total_count} flags")
print(f"Success rate: {(success_count/total_count*100):.1f}%")
print(f"Total time: {duration:.1f} seconds")
print(f"Average time per flag: {duration/total_count:.2f} seconds")