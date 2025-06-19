#!/usr/bin/env python3
"""
Job Recommendation System using TF-IDF and Cosine Similarity.
Data is sourced from MinIO.
"""

import os
import sys
import pickle
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import tempfile
import shutil
import re # Untuk pembersihan teks dasar

# --- MinIO Imports ---
try:
    from minio import Minio
    from minio.error import S3Error
    print("✓ MinIO client imported successfully.")
    MINIO_AVAILABLE = True
except ImportError:
    print("✗ MinIO client not found. Please install it: pip install minio")
    MINIO_AVAILABLE = False
    class Minio: pass # Dummy class
    class S3Error(Exception): pass # Dummy exception

# --- Konstanta Aplikasi ---
TFIDF_VECTORIZER_FILE = './models_tfidf/tfidf_vectorizer.pkl'
TFIDF_MATRIX_FILE = './models_tfidf/tfidf_matrix.pkl' # Matriks TF-IDF dari data pekerjaan
JOB_DATA_TFIDF_FILE = './models_tfidf/job_data_tfidf.pkl' # DataFrame pekerjaan untuk referensi
COSINE_SIM_MATRIX_FILE = './models_tfidf/cosine_similarity_matrix.pkl' # Opsional, bisa dihitung on-the-fly

# --- Konstanta Konfigurasi MinIO (GANTI DENGAN NILAI ANDA) ---
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
MINIO_BUCKET_NAME = "jobs"
MINIO_USE_SSL = False

class JobTfidfRecommender:
    def __init__(self):
        self.minio_client = None
        self.vectorizer = None
        self.tfidf_matrix = None # Matriks TF-IDF untuk data pekerjaan
        self.job_data_df = None  # DataFrame Pandas berisi info pekerjaan
        self.cosine_sim_matrix = None # Matriks kemiripan antar pekerjaan

        os.makedirs("./models_tfidf", exist_ok=True)

        if MINIO_AVAILABLE:
            try:
                self.minio_client = Minio(
                    MINIO_ENDPOINT,
                    access_key=MINIO_ACCESS_KEY,
                    secret_key=MINIO_SECRET_KEY,
                    secure=MINIO_USE_SSL
                )
                print(f"✓ MinIO client initialized for endpoint: {MINIO_ENDPOINT}")
            except Exception as e:
                print(f"✗ Warning: Failed to initialize MinIO client: {e}")
                self.minio_client = None
        else:
            print("✗ MinIO library not available. Data loading from MinIO will fail.")

    def _clean_text(self, text):
        """Basic text cleaning: lowercase, remove punctuation, extra spaces."""
        if not isinstance(text, str):
            return ""
        text = text.lower()
        text = re.sub(r'[^\w\s]', '', text) # Hapus punctuation
        text = re.sub(r'\s+', ' ', text).strip() # Hapus spasi berlebih
        return text

    def load_and_combine_data_from_minio(self, bucket_name=MINIO_BUCKET_NAME):
        """Downloads all CSVs from MinIO, loads, and combines them into a Pandas DataFrame."""
        if not self.minio_client:
            print("✗ MinIO client not initialized. Cannot download data.")
            return pd.DataFrame()

        print(f"Attempting to load data from MinIO bucket: '{bucket_name}'...")
        temp_dir = tempfile.mkdtemp(prefix="minio_pandas_")
        print(f"  Temporary download directory created: {temp_dir}")
        all_dataframes = []
        
        try:
            objects = self.minio_client.list_objects(bucket_name, recursive=True)
            csv_files_found = False
            for obj in objects:
                if obj.object_name.lower().endswith('.csv'):
                    csv_files_found = True
                    local_file_path = os.path.join(temp_dir, os.path.basename(obj.object_name))
                    print(f"  Downloading '{obj.object_name}' to '{local_file_path}'...")
                    try:
                        self.minio_client.fget_object(bucket_name, obj.object_name, local_file_path)
                        print(f"    ✓ Downloaded '{obj.object_name}'.")
                        df_batch = pd.read_csv(local_file_path)
                        print(f"    ✓ Loaded {len(df_batch)} records from '{os.path.basename(obj.object_name)}'.")
                        all_dataframes.append(df_batch)
                    except Exception as e_file:
                        print(f"    ✗ Error processing file '{obj.object_name}': {e_file}")
            
            if not csv_files_found:
                print(f"✗ No CSV files found in MinIO bucket '{bucket_name}'.")
                return pd.DataFrame()
            if not all_dataframes:
                print(f"✗ No data could be loaded from CSV files in bucket '{bucket_name}'.")
                return pd.DataFrame()

            print("  Combining all loaded dataframes...")
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Kolom yang akan digunakan untuk membuat 'tags'
            text_feature_cols = ['Job Title', 'Role', 'Job Description', 'skills', 'Responsibilities']
            for col in text_feature_cols:
                if col not in combined_df.columns:
                    print(f"  Warning: Column '{col}' not found. It will be created as empty.")
                    combined_df[col] = "" # Buat kolom kosong jika tidak ada
                combined_df[col] = combined_df[col].fillna('').astype(str).apply(self._clean_text)

            # Gabungkan fitur teks menjadi satu kolom 'tags'
            combined_df['tags'] = combined_df[text_feature_cols].apply(lambda row: ' '.join(row.values.astype(str)), axis=1)
            
            # Pilih kolom yang relevan untuk disimpan
            # Pastikan 'Job Id' ada, jika tidak, buat placeholder atau berikan error
            if 'Job Id' not in combined_df.columns:
                 print("  Warning: 'Job Id' column not found. Using index as ID.")
                 combined_df['Job Id'] = combined_df.index
            
            self.job_data_df = combined_df[['Job Id', 'Job Title', 'Company', 'tags']].copy()
            # Hapus duplikat berdasarkan 'tags' jika ada pekerjaan yang sangat mirip deskripsinya
            # self.job_data_df.drop_duplicates(subset=['tags'], inplace=True)
            self.job_data_df.reset_index(drop=True, inplace=True) # Reset index setelah drop

            print(f"✓ Combined data successfully: {len(self.job_data_df)} total records.")
            return self.job_data_df

        except Exception as e:
            print(f"✗ An unexpected error occurred while processing data from MinIO: {e}")
            return pd.DataFrame()
        finally:
            if os.path.exists(temp_dir):
                print(f"  Cleaning up temporary download directory: {temp_dir}")
                shutil.rmtree(temp_dir)
                print("    ✓ Temporary directory cleaned.")

    def build_tfidf_model(self):
        """Builds or loads the TF-IDF vectorizer and transforms job data."""
        if self.job_data_df is None or self.job_data_df.empty:
            print("✗ Job data is not loaded. Cannot build TF-IDF model.")
            return False

        # Coba muat model yang sudah ada
        if os.path.exists(TFIDF_VECTORIZER_FILE) and \
           os.path.exists(TFIDF_MATRIX_FILE) and \
           os.path.exists(JOB_DATA_TFIDF_FILE):
            print("Loading existing TF-IDF model and data...")
            try:
                with open(TFIDF_VECTORIZER_FILE, 'rb') as f: self.vectorizer = pickle.load(f)
                with open(TFIDF_MATRIX_FILE, 'rb') as f: self.tfidf_matrix = pickle.load(f)
                with open(JOB_DATA_TFIDF_FILE, 'rb') as f: self.job_data_df = pickle.load(f)
                
                # Opsional: Muat atau hitung ulang cosine similarity matrix
                if os.path.exists(COSINE_SIM_MATRIX_FILE):
                     with open(COSINE_SIM_MATRIX_FILE, 'rb') as f: self.cosine_sim_matrix = pickle.load(f)
                elif self.tfidf_matrix is not None:
                    print("  Calculating cosine similarity matrix...")
                    self.cosine_sim_matrix = cosine_similarity(self.tfidf_matrix)

                print("✓ TF-IDF model, matrix, and job data loaded successfully.")
                return True
            except Exception as e:
                print(f"✗ Error loading TF-IDF files: {e}. Rebuilding model...")

        print("Building new TF-IDF model...")
        # Inisialisasi TfidfVectorizer
        # stop_words='english' bisa ditambahkan jika diinginkan
        # max_df untuk mengabaikan term yang terlalu sering muncul (misal 0.95)
        # min_df untuk mengabaikan term yang terlalu jarang muncul (misal 2 atau 0.01)
        # ngram_range=(1,2) bisa menangkap bi-grams juga
        self.vectorizer = TfidfVectorizer(stop_words='english', max_df=0.9, min_df=5) 
        
        # Fit dan transform kolom 'tags'
        # Pastikan tidak ada NaN di 'tags'
        self.job_data_df['tags'] = self.job_data_df['tags'].fillna('')
        self.tfidf_matrix = self.vectorizer.fit_transform(self.job_data_df['tags'])
        
        print(f"✓ TF-IDF matrix created with shape: {self.tfidf_matrix.shape}")

        # Hitung cosine similarity matrix antar semua pekerjaan
        print("  Calculating cosine similarity matrix...")
        self.cosine_sim_matrix = cosine_similarity(self.tfidf_matrix)
        print(f"✓ Cosine similarity matrix created with shape: {self.cosine_sim_matrix.shape}")

        # Simpan model dan data
        self.save_tfidf_model()
        return True

    def save_tfidf_model(self):
        """Saves the TF-IDF vectorizer, matrix, and job data."""
        if self.vectorizer and self.tfidf_matrix is not None and self.job_data_df is not None:
            print("Saving TF-IDF model, matrix, and job data...")
            with open(TFIDF_VECTORIZER_FILE, 'wb') as f: pickle.dump(self.vectorizer, f)
            with open(TFIDF_MATRIX_FILE, 'wb') as f: pickle.dump(self.tfidf_matrix, f)
            with open(JOB_DATA_TFIDF_FILE, 'wb') as f: pickle.dump(self.job_data_df, f)
            if self.cosine_sim_matrix is not None:
                with open(COSINE_SIM_MATRIX_FILE, 'wb') as f: pickle.dump(self.cosine_sim_matrix, f)
            print("✓ TF-IDF artifacts saved successfully.")
        else:
            print("✗ Nothing to save. Vectorizer, matrix, or job_data_df is missing.")

    def recommend_jobs_by_query(self, user_query, top_n=5):
        """Recommends jobs based on a user query using TF-IDF."""
        if self.vectorizer is None or self.tfidf_matrix is None or self.job_data_df is None:
            print("✗ TF-IDF model not ready. Please build or load the model first.")
            return pd.DataFrame()

        print(f"\nRecommending jobs for query: '{user_query}'")
        cleaned_query = self._clean_text(user_query)
        query_vector = self.vectorizer.transform([cleaned_query])
        
        # Hitung cosine similarity antara query dan semua pekerjaan
        cosine_similarities_query = cosine_similarity(query_vector, self.tfidf_matrix).flatten()
        
        # Dapatkan top_n pekerjaan yang paling mirip
        # Menggunakan np.argsort untuk mendapatkan indeks, lalu membaliknya untuk descending order
        top_n_indices = np.argsort(cosine_similarities_query)[::-1][:top_n]
        
        results_df = self.job_data_df.iloc[top_n_indices].copy()
        results_df['similarity_score'] = cosine_similarities_query[top_n_indices]
        
        print(f"✓ Found {len(results_df)} similar jobs for the query.")
        return results_df[['Job Id', 'Job Title', 'Company', 'similarity_score', 'tags']]

    def recommend_jobs_by_job_id(self, job_id, top_n=5):
        """Recommends jobs similar to a given job_id using precomputed cosine_sim_matrix."""
        if self.cosine_sim_matrix is None or self.job_data_df is None:
            print("✗ Cosine similarity matrix or job data not ready.")
            return pd.DataFrame()

        try:
            # Dapatkan index dari job_id di DataFrame
            idx_list = self.job_data_df[self.job_data_df['Job Id'] == job_id].index
            if not idx_list.any(): # Jika Job Id tidak ditemukan
                print(f"✗ Job Id '{job_id}' not found in the dataset.")
                return pd.DataFrame()
            idx = idx_list[0]
            target_job_title = self.job_data_df.iloc[idx]['Job Title']
            print(f"\nRecommending jobs similar to '{target_job_title}' (ID: {job_id})...")
        except Exception as e:
            print(f"✗ Error finding job with ID '{job_id}': {e}")
            return pd.DataFrame()

        # Dapatkan skor kemiripan dari matriks yang sudah dihitung
        sim_scores = list(enumerate(self.cosine_sim_matrix[idx]))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
        
        # Ambil top N, kecualikan diri sendiri (indeks pertama setelah sorting)
        sim_scores = sim_scores[1:top_n+1] 
        job_indices = [i[0] for i in sim_scores]
        
        results_df = self.job_data_df.iloc[job_indices].copy()
        results_df['similarity_score'] = [s[1] for s in sim_scores] # Ambil skor similarity

        print(f"✓ Found {len(results_df)} similar jobs.")
        return results_df[['Job Id', 'Job Title', 'Company', 'similarity_score', 'tags']]


def main():
    print("=" * 60)
    print("Job Recommendation System using TF-IDF")
    print("=" * 60)

    if not MINIO_AVAILABLE:
        print("✗ MinIO library is not installed. This program requires MinIO to fetch data.")
        print("=" * 60)
        return

    recommender = JobTfidfRecommender()

    # 1. Muat data dari MinIO
    # Jika model sudah ada dan Anda tidak ingin memuat ulang dari MinIO setiap kali,
    # Anda bisa menambahkan logika untuk melewati langkah ini jika file model sudah ada.
    # Namun, untuk memastikan data terbaru, biasanya data dimuat ulang.
    initial_job_data = recommender.load_and_combine_data_from_minio()
    if initial_job_data.empty:
        print("✗ Critical Error: No data loaded from MinIO. Exiting.")
        return

    # 2. Bangun (atau muat) model TF-IDF
    if not recommender.build_tfidf_model():
        print("✗ Critical Error: Failed to build or load TF-IDF model. Exiting.")
        return

    # 3. Contoh Rekomendasi berdasarkan Kueri Pengguna
    print("\n" + "="*40)
    print("Example: Recommendations based on User Query")
    print("="*40)
    
    user_query1 = "experienced web developer proficient in javascript and react"
    recommendations1 = recommender.recommend_jobs_by_query(user_query1, top_n=3)
    if not recommendations1.empty:
        print(f"\nTop 3 recommendations for query '{user_query1}':")
        for _, row in recommendations1.iterrows():
            print(f"  - ID: {row['Job Id']}, Title: {row['Job Title']}, Company: {row['Company']} (Score: {row['similarity_score']:.4f})")
            # print(f"    Tags: {row['tags'][:150]}...") # Uncomment untuk melihat tags

    print("-" * 30)
    user_query2 = "social media marketing manager"
    recommendations2 = recommender.recommend_jobs_by_query(user_query2, top_n=3)
    if not recommendations2.empty:
        print(f"\nTop 3 recommendations for query '{user_query2}':")
        for _, row in recommendations2.iterrows():
            print(f"  - ID: {row['Job Id']}, Title: {row['Job Title']}, Company: {row['Company']} (Score: {row['similarity_score']:.4f})")

    # 4. Contoh Rekomendasi berdasarkan Pekerjaan Serupa (Item-to-Item)
    # Ambil Job Id acak dari dataset untuk contoh
    if recommender.job_data_df is not None and not recommender.job_data_df.empty:
        print("\n" + "="*40)
        print("Example: Recommendations based on Similar Job")
        print("="*40)
        sample_job_id_for_rec = recommender.job_data_df['Job Id'].sample(1).iloc[0]
        
        # Pastikan sample_job_id_for_rec valid (bukan NaN atau tipe yang salah)
        if pd.notna(sample_job_id_for_rec):
            try:
                # Jika Job Id numerik, konversi ke tipe data yang sesuai jika perlu
                # Dalam contoh dataset, Job Id adalah numerik tapi bisa juga string.
                # Jika Anda menggunakan index sebagai Job Id, pastikan tipenya int.
                # sample_job_id_for_rec = int(sample_job_id_for_rec) # Jika Job Id harus int
                pass
            except ValueError:
                print(f"Warning: Sample Job Id '{sample_job_id_for_rec}' is not a valid integer. Skipping similar job recommendation.")
            else:
                item_based_recs = recommender.recommend_jobs_by_job_id(sample_job_id_for_rec, top_n=3)
                if not item_based_recs.empty:
                    # print(f"\nTop 3 jobs similar to Job ID '{sample_job_id_for_rec}':") (Sudah dicetak di dalam fungsi)
                    for _, row in item_based_recs.iterrows():
                        print(f"  - ID: {row['Job Id']}, Title: {row['Job Title']}, Company: {row['Company']} (Score: {row['similarity_score']:.4f})")
        else:
            print("Could not get a valid sample Job Id for item-based recommendation example.")


    print("\n" + "=" * 60)
    print("✅ Process completed!")
    print(f"TF-IDF model artifacts are saved in/loaded from './models_tfidf/' directory.")
    print("=" * 60)

if __name__ == "__main__":
    main()