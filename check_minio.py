#!/usr/bin/env python3
"""
Script sederhana untuk cek isi MinIO bucket flags
"""

from minio import Minio
import os

def check_minio():
    """Cek isi MinIO bucket flags"""
    
    # Inisialisasi MinIO client
    minio_client = Minio(
        "localhost:9000",
        access_key="minio_access_key",
        secret_key="minio_secret_key",
        secure=False
    )
    
    bucket_name = "flags"
    
    try:
        # Cek apakah bucket ada
        if not minio_client.bucket_exists(bucket_name):
            print(f"❌ Bucket '{bucket_name}' tidak ditemukan")
            print("💡 Jalankan: python setup_flags.py")
            return False
        
        print(f"✅ Bucket '{bucket_name}' ditemukan")
        
        # List semua object dalam bucket
        objects = list(minio_client.list_objects(bucket_name))
        
        if not objects:
            print("❌ Bucket kosong")
            print("💡 Jalankan: python setup_flags.py")
            return False
        
        print(f"📊 Total files: {len(objects)}")
        
        # Tampilkan beberapa file sebagai contoh
        print(f"\n📋 Sample files:")
        for i, obj in enumerate(objects[:10]):
            print(f"   • {obj.object_name}")
        
        if len(objects) > 10:
            print(f"   ... dan {len(objects) - 10} file lainnya")
        
        # Test beberapa flag penting
        test_flags = ['id.svg', 'us.svg', 'sg.svg', 'my.svg', 'in.svg']
        print(f"\n🧪 Testing important flags:")
        
        for flag in test_flags:
            try:
                obj_info = minio_client.stat_object(bucket_name, flag)
                print(f"   ✅ {flag}: {obj_info.size} bytes")
            except Exception as e:
                print(f"   ❌ {flag}: {str(e)}")
        
        print(f"\n🎉 MinIO bucket ready!")
        print(f"🌐 Flags dapat diakses via: http://localhost:4000/flag/<country_code>.svg")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("💡 Pastikan MinIO server running")
        return False

if __name__ == "__main__":
    print("🔍 Checking MinIO Flags Bucket")
    print("=" * 35)
    check_minio() 