#!/usr/bin/env python3
"""
MinIO utilities for Job Recommendation System
"""

from minio import Minio
from minio.error import S3Error
import pandas as pd

class MinIOUtils:
    def __init__(self, endpoint="localhost:9000", access_key="minio_access_key", secret_key="minio_secret_key"):
        """Initialize MinIO client"""
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
    
    def list_buckets(self):
        """List all available buckets"""
        try:
            buckets = self.client.list_buckets()
            print("Available buckets:")
            for bucket in buckets:
                print(f"  - {bucket.name} (created: {bucket.creation_date})")
            return [bucket.name for bucket in buckets]
        except S3Error as e:
            print(f"Error listing buckets: {e}")
            return []
    
    def list_objects(self, bucket_name, prefix="", show_details=True):
        """List objects in a bucket"""
        try:
            objects = list(self.client.list_objects(bucket_name, prefix=prefix, recursive=True))
            
            if show_details:
                print(f"\nObjects in bucket '{bucket_name}':")
                if prefix:
                    print(f"  (filtered by prefix: '{prefix}')")
                
                total_size = 0
                for obj in objects:
                    size_mb = obj.size / (1024 * 1024)
                    total_size += obj.size
                    print(f"  - {obj.object_name}")
                    print(f"    Size: {size_mb:.2f} MB")
                    print(f"    Modified: {obj.last_modified}")
                    print()
                
                print(f"Total objects: {len(objects)}")
                print(f"Total size: {total_size / (1024 * 1024):.2f} MB")
            
            return [obj.object_name for obj in objects]
            
        except S3Error as e:
            print(f"Error listing objects: {e}")
            return []
    
    def download_object(self, bucket_name, object_name, local_path):
        """Download an object from MinIO"""
        try:
            self.client.fget_object(bucket_name, object_name, local_path)
            print(f"Downloaded {object_name} to {local_path}")
            return True
        except S3Error as e:
            print(f"Error downloading {object_name}: {e}")
            return False
    
    def get_batch_info(self, bucket_name="jobs"):
        """Get information about job batches"""
        try:
            objects = list(self.client.list_objects(bucket_name, recursive=True))
            csv_files = [obj for obj in objects if obj.object_name.endswith('.csv')]
            
            print(f"\nJob batch analysis for bucket '{bucket_name}':")
            print(f"Total CSV files: {len(csv_files)}")
            
            # Group by date pattern
            batch_dates = {}
            for obj in csv_files:
                # Extract date from filename (e.g., jobs_batch_20250613_032443.csv)
                parts = obj.object_name.split('_')
                if len(parts) >= 3:
                    date_part = parts[2]  # 20250613
                    if date_part not in batch_dates:
                        batch_dates[date_part] = []
                    batch_dates[date_part].append(obj)
            
            print(f"Batch dates found: {len(batch_dates)}")
            for date, files in sorted(batch_dates.items()):
                total_size = sum(f.size for f in files)
                print(f"  {date}: {len(files)} files, {total_size / (1024 * 1024):.2f} MB")
            
            return csv_files
            
        except S3Error as e:
            print(f"Error analyzing batches: {e}")
            return []
    
    def test_connection(self):
        """Test MinIO connection"""
        try:
            buckets = self.client.list_buckets()
            print(f"✓ Successfully connected to MinIO")
            print(f"✓ Found {len(buckets)} buckets")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to MinIO: {e}")
            return False

def main():
    """Test MinIO utilities"""
    print("=" * 60)
    print("MinIO Utilities - Connection Test")
    print("=" * 60)
    
    minio_utils = MinIOUtils()
    
    # Test connection
    if not minio_utils.test_connection():
        print("Cannot proceed without MinIO connection")
        return
    
    # List buckets
    minio_utils.list_buckets()
    
    # Analyze job batches
    minio_utils.get_batch_info("jobs")
    
    # List specific objects
    print("\n" + "=" * 40)
    print("Detailed file listing:")
    minio_utils.list_objects("jobs", prefix="jobs_batch_")

if __name__ == "__main__":
    main()