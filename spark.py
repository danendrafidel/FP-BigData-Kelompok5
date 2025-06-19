#!/usr/bin/env python3
"""
Job Recommendation System using TF-IDF and Cosine Similarity.
Data is sourced from MinIO.
Handles large number of items by calculating item-to-item similarity on-demand.
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
    print("[OK] MinIO client imported successfully.")
    MINIO_AVAILABLE = True
except ImportError:
    print("[ERROR] MinIO client not found. Please install it: pip install minio")
    MINIO_AVAILABLE = False
    class Minio: pass # Dummy class
    class S3Error(Exception): pass # Dummy exception

# --- Konstanta Aplikasi ---
TFIDF_VECTORIZER_FILE = './models_tfidf/tfidf_vectorizer.pkl'
TFIDF_MATRIX_FILE = './models_tfidf/tfidf_matrix.pkl' # Matriks TF-IDF dari data pekerjaan
JOB_DATA_TFIDF_FILE = './models_tfidf/job_data_tfidf.pkl' # DataFrame pekerjaan untuk referensi
# COSINE_SIM_MATRIX_FILE sudah tidak digunakan untuk penyimpanan matriks penuh

# --- Konstanta Konfigurasi MinIO (GANTI DENGAN NILAI ANDA) ---
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minio_access_key"
MINIO_SECRET_KEY = "minio_secret_key"
MINIO_BUCKET_NAME = "jobs" # Pastikan ini sesuai dengan bucket Anda
MINIO_USE_SSL = False

class JobTfidfRecommender:
    def __init__(self):
        self.minio_client = None
        self.vectorizer = None
        self.tfidf_matrix = None # Matriks TF-IDF untuk data pekerjaan
        self.job_data_df = None  # DataFrame Pandas berisi info pekerjaan
        # self.cosine_sim_matrix = None # Tidak lagi menyimpan matriks penuh

        os.makedirs("./models_tfidf", exist_ok=True)

        if MINIO_AVAILABLE:
            try:
                self.minio_client = Minio(
                    MINIO_ENDPOINT,
                    access_key=MINIO_ACCESS_KEY,
                    secret_key=MINIO_SECRET_KEY,
                    secure=MINIO_USE_SSL
                )
                print(f"[OK] MinIO client initialized for endpoint: {MINIO_ENDPOINT}")
            except Exception as e:
                print(f"[ERROR] Warning: Failed to initialize MinIO client: {e}")
                self.minio_client = None
        else:
            print("[ERROR] MinIO library not available. Data loading from MinIO will fail.")

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
            print("[ERROR] MinIO client not initialized. Cannot download data.")
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
                        print(f"    [OK] Downloaded '{obj.object_name}'.")
                        # Coba baca dengan error handling yang lebih baik
                        # Untuk Pandas >= 1.3.0, gunakan on_bad_lines='warn' atau 'skip'
                        # Untuk versi lama, error_bad_lines=False (untuk skip) atau warn_bad_lines=True
                        try:
                            df_batch = pd.read_csv(local_file_path, on_bad_lines='warn')
                        except AttributeError: # Untuk Pandas versi lama
                             df_batch = pd.read_csv(local_file_path, error_bad_lines=False, warn_bad_lines=True)
                        print(f"    [OK] Loaded {len(df_batch)} records from '{os.path.basename(obj.object_name)}'.")
                        all_dataframes.append(df_batch)
                    except Exception as e_file:
                        print(f"    [ERROR] Error processing file '{obj.object_name}': {e_file}")
            
            if not csv_files_found:
                print(f"[ERROR] No CSV files found in MinIO bucket '{bucket_name}'.")
                return pd.DataFrame()
            if not all_dataframes:
                print(f"[ERROR] No data could be loaded from CSV files in bucket '{bucket_name}'.")
                return pd.DataFrame()

            print("  Combining all loaded dataframes...")
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Kolom yang akan digunakan untuk membuat 'tags'
            text_feature_cols = ['Job Title', 'Role', 'Job Description', 'skills', 'Responsibilities']
            # Bersihkan dan gabungkan kolom untuk 'tags'
            for col_name in text_feature_cols:
                if col_name not in combined_df.columns:
                    print(f"  Warning: Column '{col_name}' for 'tags' not found. It will be treated as empty for tag generation.")
                    combined_df[col_name] = "" # Pastikan kolom ada untuk .apply
                combined_df[col_name] = combined_df[col_name].fillna('').astype(str).apply(self._clean_text)
            
            combined_df['tags'] = combined_df.apply(
                lambda row: ' '.join([row[col_name] for col_name in text_feature_cols if pd.notna(row[col_name]) and row[col_name].strip()]),
                axis=1
            )
            
            all_desired_original_cols = [
                'Job Id', 'Experience', 'Qualifications', 'Salary Range', 'location', 
                'Country', 'latitude', 'longitude', 'Work Type', 'Company Size', 
                'Job Posting Date', 'Preference', 'Contact Person', 'Contact', 
                'Job Title', 'Role', 'Job Portal', 'Job Description', 'Benefits', 
                'skills', 'Responsibilities', 'Company', 'Company Profile'
            ]
            cols_to_save = [col for col in combined_df.columns if col in all_desired_original_cols]
            
            if 'Job Id' not in cols_to_save:
                if 'Job Id' in combined_df.columns:
                    cols_to_save.insert(0, 'Job Id')
                else:
                    print("  Warning: 'Job Id' column not found in source or desired list. Using DataFrame index as 'Job Id'.")
                    combined_df['Job Id'] = combined_df.index
                    if 'Job Id' not in cols_to_save:
                         cols_to_save.insert(0, 'Job Id')

            if 'tags' not in cols_to_save and 'tags' in combined_df.columns:
                cols_to_save.append('tags')
            
            cols_to_save = sorted(list(set(cols_to_save)), key=lambda x: all_desired_original_cols.index(x) if x in all_desired_original_cols else float('inf'))


            missing_original_cols = set(all_desired_original_cols) - set(combined_df.columns)
            if missing_original_cols:
                print(f"  Info: The following original columns were not found in any loaded CSV files: {missing_original_cols}")

            if not cols_to_save:
                print("  Error: No columns were selected to be saved in job_data_df. This is unexpected.")
                return pd.DataFrame()

            self.job_data_df = combined_df[cols_to_save].copy()
            self.job_data_df.reset_index(drop=True, inplace=True)

            print(f"[OK] Combined data successfully: {len(self.job_data_df)} total records. Columns saved: {self.job_data_df.columns.tolist()}")
            return self.job_data_df

        except Exception as e:
            print(f"[ERROR] An unexpected error occurred while processing data from MinIO: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
        finally:
            if os.path.exists(temp_dir):
                print(f"  Cleaning up temporary download directory: {temp_dir}")
                shutil.rmtree(temp_dir)
                print("    [OK] Temporary directory cleaned.")
    
    def build_tfidf_model(self):
        """Builds or loads the TF-IDF vectorizer and transforms job data."""
        if self.job_data_df is None or self.job_data_df.empty:
            print("[ERROR] Job data is not loaded. Cannot build TF-IDF model.")
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
                
                print("[OK] TF-IDF model, matrix, and job data loaded successfully (cosine similarity matrix is not pre-calculated).")
                return True
            except Exception as e:
                print(f"[ERROR] Error loading TF-IDF files: {e}. Rebuilding model...")

        print("Building new TF-IDF model...")
        # Parameter TfidfVectorizer:
        # stop_words='english': Menghapus kata-kata umum dalam bahasa Inggris.
        # max_df=0.9: Mengabaikan term yang muncul di lebih dari 90% dokumen (terlalu umum).
        # min_df=5: Mengabaikan term yang muncul di kurang dari 5 dokumen (terlalu jarang/noise).
        # ngram_range=(1,2): Mempertimbangkan unigram (satu kata) dan bigram (dua kata berdampingan).
        self.vectorizer = TfidfVectorizer(stop_words='english', max_df=0.9, min_df=5, ngram_range=(1,2)) 
        
        if 'tags' not in self.job_data_df.columns:
            print("[ERROR] Error: 'tags' column is missing from job_data_df. Cannot build TF-IDF model.")
            return False
        self.job_data_df['tags'] = self.job_data_df['tags'].fillna('') # Pastikan tidak ada NaN
        
        self.tfidf_matrix = self.vectorizer.fit_transform(self.job_data_df['tags'])
        print(f"[OK] TF-IDF matrix created with shape: {self.tfidf_matrix.shape}")

        # Tidak lagi menghitung cosine_sim_matrix penuh di sini untuk menghemat memori
        # self.cosine_sim_matrix = cosine_similarity(self.tfidf_matrix)
        # print(f"[OK] Cosine similarity matrix created with shape: {self.cosine_sim_matrix.shape}")

        self.save_tfidf_model()
        return True

    def save_tfidf_model(self):
        """Saves the TF-IDF vectorizer, matrix, and job data."""
        if self.vectorizer and self.tfidf_matrix is not None and \
           self.job_data_df is not None and not self.job_data_df.empty:
            print("Saving TF-IDF model, matrix, and job data...")
            with open(TFIDF_VECTORIZER_FILE, 'wb') as f: pickle.dump(self.vectorizer, f)
            with open(TFIDF_MATRIX_FILE, 'wb') as f: pickle.dump(self.tfidf_matrix, f)
            with open(JOB_DATA_TFIDF_FILE, 'wb') as f: pickle.dump(self.job_data_df, f)
            # Tidak lagi menyimpan cosine_sim_matrix penuh
            print("[OK] TF-IDF artifacts (vectorizer, tfidf_matrix, job_data) saved successfully.")
        else:
            print("[ERROR] Nothing to save. Vectorizer, matrix, or job_data_df is missing or empty.")

    def recommend_jobs_by_query(self, user_query, top_n=5):
        """Recommends jobs based on a user query using TF-IDF."""
        if self.vectorizer is None or self.tfidf_matrix is None or \
           self.job_data_df is None or self.job_data_df.empty:
            print("[ERROR] TF-IDF model not ready. Please build or load the model first.")
            return pd.DataFrame()

        print(f"\nRecommending jobs for query: '{user_query}'")
        cleaned_query = self._clean_text(user_query)
        query_vector = self.vectorizer.transform([cleaned_query])
        
        cosine_similarities_query = cosine_similarity(query_vector, self.tfidf_matrix).flatten()
        top_n_indices = np.argsort(cosine_similarities_query)[::-1][:top_n]
        
        results_df = self.job_data_df.iloc[top_n_indices].copy()
        results_df['similarity_score'] = cosine_similarities_query[top_n_indices]
        
        print(f"[OK] Found {len(results_df)} similar jobs for the query.")
        return results_df # Mengembalikan semua kolom yang ada di job_data_df

    def recommend_jobs_by_job_id(self, job_id, top_n=5):
        """Recommends jobs similar to a given job_id by calculating similarity on-demand."""
        if self.tfidf_matrix is None or \
           self.job_data_df is None or self.job_data_df.empty:
            print("[ERROR] TF-IDF matrix or job data not ready for item-based recommendation.")
            return pd.DataFrame()

        # Konversi tipe job_id agar sesuai dengan tipe di DataFrame
        job_id_type_in_df = self.job_data_df['Job Id'].dtype
        try:
            if pd.api.types.is_numeric_dtype(job_id_type_in_df) and not isinstance(job_id, (int, float, np.number)):
                job_id_converted = job_id_type_in_df.type(job_id)
            elif pd.api.types.is_string_dtype(job_id_type_in_df) and not isinstance(job_id, str):
                job_id_converted = str(job_id)
            else:
                job_id_converted = job_id # Tipe sudah sesuai
        except ValueError:
            print(f"[ERROR] Could not convert input job_id '{job_id}' to match DataFrame 'Job Id' type ({job_id_type_in_df}).")
            return pd.DataFrame()
        
        try:
            idx_list = self.job_data_df[self.job_data_df['Job Id'] == job_id_converted].index
            if not idx_list.any():
                print(f"[ERROR] Job Id '{job_id_converted}' not found in the dataset.")
                return pd.DataFrame()
            idx = idx_list[0]
            target_job_title = self.job_data_df.iloc[idx]['Job Title']
            print(f"\nRecommending jobs similar to '{target_job_title}' (ID: {job_id_converted})...")

            target_job_vector = self.tfidf_matrix[idx]
            # Hitung kemiripan on-demand dengan semua pekerjaan lain
            sim_scores_for_target = cosine_similarity(target_job_vector, self.tfidf_matrix).flatten()
            
            sim_scores_list = list(enumerate(sim_scores_for_target))
            sim_scores_list = sorted(sim_scores_list, key=lambda x: x[1], reverse=True)
            # Abaikan item itu sendiri (skor tertinggi akan 1.0 dengan dirinya sendiri)
            sim_scores_list = [s for s in sim_scores_list if s[0] != idx][:top_n]
            # Jika item itu sendiri ada di top N (selain di indeks 0 setelah sorting karena mungkin ada duplikat sempurna),
            # kita bisa juga mengambil sim_scores_list[1:top_n+1] jika yakin item pertama adalah dirinya sendiri.
            # Namun, lebih aman filter berdasarkan indeks.

            job_indices = [i[0] for i in sim_scores_list]
            
            results_df = self.job_data_df.iloc[job_indices].copy()
            results_df['similarity_score'] = [s[1] for s in sim_scores_list]

            print(f"[OK] Found {len(results_df)} similar jobs.")
            return results_df # Mengembalikan semua kolom
        except Exception as e:
            print(f"[ERROR] Error in recommend_jobs_by_job_id: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()


def main():
    print("=" * 60)
    print("Job Recommendation System using TF-IDF")
    print("=" * 60)

    if not MINIO_AVAILABLE:
        print("[ERROR] MinIO library is not installed. This program requires MinIO to fetch data.")
        print("=" * 60)
        return

    recommender = JobTfidfRecommender()

    initial_job_data = recommender.load_and_combine_data_from_minio()
    if initial_job_data.empty:
        print("[ERROR] Critical Error: No data loaded from MinIO. Exiting.")
        return

    if not recommender.build_tfidf_model():
        print("[ERROR] Critical Error: Failed to build or load TF-IDF model. Exiting.")
        return

    print("\n" + "="*40)
    print("Example: Recommendations based on User Query")
    print("="*40)
    
    user_query1 = "experienced web developer proficient in javascript and react"
    recommendations1 = recommender.recommend_jobs_by_query(user_query1, top_n=3)
    if not recommendations1.empty:
        print(f"\nTop 3 recommendations for query '{user_query1}':")
        for _, row in recommendations1.iterrows():
            # Menggunakan .get() untuk keamanan jika suatu saat ada kolom yang hilang
            print(f"  - ID: {row.get('Job Id', 'N/A')}, "
                  f"Title: {row.get('Job Title', 'N/A')}, "
                  f"Company: {row.get('Company', 'N/A')}, "
                  f"Role: {row.get('Role', 'N/A')} " # Contoh penambahan kolom Role
                  f"(Score: {row.get('similarity_score', 0.0):.4f})")

    print("-" * 30)
    user_query2 = "social media marketing manager"
    recommendations2 = recommender.recommend_jobs_by_query(user_query2, top_n=3)
    if not recommendations2.empty:
        print(f"\nTop 3 recommendations for query '{user_query2}':")
        for _, row in recommendations2.iterrows():
            print(f"  - ID: {row.get('Job Id', 'N/A')}, "
                  f"Title: {row.get('Job Title', 'N/A')}, "
                  f"Company: {row.get('Company', 'N/A')}, "
                  f"Role: {row.get('Role', 'N/A')} "
                  f"(Score: {row.get('similarity_score', 0.0):.4f})")


    if recommender.job_data_df is not None and not recommender.job_data_df.empty:
        print("\n" + "="*40)
        print("Example: Recommendations based on Similar Job")
        print("="*40)
        
        if 'Job Id' in recommender.job_data_df.columns and not recommender.job_data_df['Job Id'].dropna().empty:
            sample_job_id_for_rec = recommender.job_data_df['Job Id'].dropna().sample(1).iloc[0]
            
            if pd.notna(sample_job_id_for_rec):
                item_based_recs = recommender.recommend_jobs_by_job_id(sample_job_id_for_rec, top_n=3)
                if not item_based_recs.empty:
                    for _, row in item_based_recs.iterrows():
                        print(f"  - ID: {row.get('Job Id', 'N/A')}, "
                              f"Title: {row.get('Job Title', 'N/A')}, "
                              f"Company: {row.get('Company', 'N/A')}, "
                              f"Role: {row.get('Role', 'N/A')} "
                              f"(Score: {row.get('similarity_score', 0.0):.4f})")
            else:
                print("Could not get a valid sample Job Id for item-based recommendation example (sample was NaN).")
        else:
             print("Could not get a sample Job Id: 'Job Id' column missing, empty, or all NaNs.")


    print("\n" + "=" * 60)
    print("[DONE] Process completed!")
    print(f"TF-IDF model artifacts are saved in/loaded from './models_tfidf/' directory.")
    if recommender.job_data_df is not None:
        print(f"Columns available in loaded/saved job_data_df: {recommender.job_data_df.columns.tolist()}")
    else:
        print("job_data_df is not available.")
    print("=" * 60)

if __name__ == "__main__":
    main()