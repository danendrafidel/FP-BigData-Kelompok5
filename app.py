#!/usr/bin/env python3
from flask import Flask, request, jsonify
from flask import render_template
import pickle
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re
import os
import requests # <-- BARU: Untuk membuat permintaan HTTP ke API bendera

app = Flask(__name__)

# --- Path ke file model ---
MODEL_DIR = './models_tfidf/'
TFIDF_VECTORIZER_FILE = os.path.join(MODEL_DIR, 'tfidf_vectorizer.pkl')
TFIDF_MATRIX_FILE = os.path.join(MODEL_DIR, 'tfidf_matrix.pkl')
JOB_DATA_TFIDF_FILE = os.path.join(MODEL_DIR, 'job_data_tfidf.pkl')
COSINE_SIM_MATRIX_FILE = os.path.join(MODEL_DIR, 'cosine_similarity_matrix.pkl')

# --- Variabel global untuk menyimpan model dan data ---
vectorizer = None
tfidf_matrix_jobs = None
job_data_df = None
cosine_sim_matrix_jobs = None
country_code_map = {} # <-- BARU: Untuk menyimpan mapping nama negara ke kode

def _clean_text(text):
    """Basic text cleaning: lowercase, remove punctuation, extra spaces."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def load_models():
    """Memuat semua model dan data yang diperlukan dari file pickle."""
    global vectorizer, tfidf_matrix_jobs, job_data_df, cosine_sim_matrix_jobs
    print("Loading TF-IDF models and data...")
    try:
        with open(TFIDF_VECTORIZER_FILE, 'rb') as f:
            vectorizer = pickle.load(f)
        print(f"✓ Vectorizer loaded from {TFIDF_VECTORIZER_FILE}")

        with open(TFIDF_MATRIX_FILE, 'rb') as f:
            tfidf_matrix_jobs = pickle.load(f)
        print(f"✓ TF-IDF matrix loaded from {TFIDF_MATRIX_FILE}")

        with open(JOB_DATA_TFIDF_FILE, 'rb') as f:
            job_data_df = pickle.load(f)
        print(f"✓ Job data DataFrame loaded from {JOB_DATA_TFIDF_FILE}")

        job_data_df = job_data_df.replace({np.nan: None})
        print("✓ DataFrame sanitized (NaN values converted to None).")

        if os.path.exists(COSINE_SIM_MATRIX_FILE):
            with open(COSINE_SIM_MATRIX_FILE, 'rb') as f:
                cosine_sim_matrix_jobs = pickle.load(f)
            print(f"✓ Cosine similarity matrix loaded from {COSINE_SIM_MATRIX_FILE}")
        else:
            print(f"Warning: Cosine similarity matrix file not found at {COSINE_SIM_MATRIX_FILE}.")

        print("✓ All models and data loaded successfully.")
        return True
    except FileNotFoundError as e:
        print(f"✗ Error: Model file not found: {e}.")
        return False
    except Exception as e:
        print(f"✗ An unexpected error occurred while loading models: {e}")
        return False

# <-- FUNGSI BARU: Untuk memuat kode negara ---
def load_country_codes():
    """Mengambil dan memproses kode negara dari API flagcdn."""
    global country_code_map
    CODES_URL = 'https://flagcdn.com/en/codes.json'
    print("Loading country codes...")
    try:
        response = requests.get(CODES_URL, timeout=10)
        response.raise_for_status() # Akan error jika status code bukan 2xx
        
        # API memberikan format {code: name}, kita balik menjadi {name: code}
        # untuk pencarian yang lebih mudah.
        data = response.json()
        # Kita juga akan membuat nama negara menjadi lowercase untuk pencocokan yang lebih baik
        country_code_map = {name.lower(): code for code, name in data.items()}
        
        print(f"✓ Successfully loaded and processed {len(country_code_map)} country codes.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching country codes: {e}")
        return False
    except Exception as e:
        print(f"✗ An unexpected error occurred while processing country codes: {e}")
        return False

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/recommend/query', methods=['POST'])
def recommend_by_query():
    """
    Endpoint untuk mendapatkan rekomendasi pekerjaan berdasarkan kueri teks.
    """
    # ▼▼▼ BARU: Tetapkan batas ambang kemiripan ▼▼▼
    # Anda bisa menyesuaikan nilai ini. 0.1 adalah awal yang baik.
    # Artinya, pekerjaan hanya akan direkomendasikan jika kemiripannya >= 10%.
    SIMILARITY_THRESHOLD = 0.1 

    if vectorizer is None or tfidf_matrix_jobs is None or job_data_df is None:
        return jsonify({"error": "Models not loaded. Please try again later or check server logs."}), 500

    try:
        data = request.get_json()
        user_query = data.get('query')
        top_n = data.get('top_n', 5)

        if not user_query:
            return jsonify({"error": "Query parameter is missing."}), 400
        if not isinstance(top_n, int) or top_n <= 0:
            return jsonify({"error": "top_n parameter must be a positive integer."}), 400

        cleaned_query = _clean_text(user_query)
        query_vector = vectorizer.transform([cleaned_query])
        
        cosine_similarities = cosine_similarity(query_vector, tfidf_matrix_jobs).flatten()
        
        # Dapatkan semua indeks, diurutkan dari yang paling mirip ke yang paling tidak mirip
        sorted_indices = np.argsort(cosine_similarities)[::-1]
        
        recommendations = []
        # ▼▼▼ LOGIKA BARU: Loop dan filter berdasarkan threshold ▼▼▼
        for idx in sorted_indices:
            # 1. Hentikan jika sudah mencapai jumlah top_n
            if len(recommendations) >= top_n:
                break
            
            # 2. Hentikan jika kemiripan sudah di bawah ambang batas
            if cosine_similarities[idx] < SIMILARITY_THRESHOLD:
                break
                
            # Jika lolos kedua kondisi di atas, tambahkan ke rekomendasi
            job_info = job_data_df.iloc[idx]
            
            # (Logika untuk mendapatkan flag_url tetap sama)
            DEFAULT_GLOBE_URL = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/icons/globe2.svg"
            country_name = job_info.get('Country', '').strip().lower()
            country_code = country_code_map.get(country_name)
            flag_url = f"https://flagcdn.com/w40/{country_code}.png" if country_code else DEFAULT_GLOBE_URL
            
            recommendations.append({
                "job_id": str(job_info['Job Id']),
                "title": job_info['Job Title'],
                "company": job_info['Company'],
                'experience': job_info['Experience'],
                'preference': job_info['Preference'],
                'salary': job_info['Salary Range'],
                'location': job_info['location'],
                'work_type': job_info['Work Type'],
                'skills': job_info['skills'],
                'contact': job_info['Contact'],
                'contact_person': job_info['Contact Person'],
                'desc': job_info['Job Description'],
                'qualification': job_info['Qualifications'],
                'country': job_info['Country'],
                'flag_url': flag_url,
                "similarity_score": round(float(cosine_similarities[idx]), 4)
            })
        
        return jsonify({"query": user_query, "recommendations": recommendations})

    except Exception as e:
        app.logger.error(f"Error in /recommend/query: {e}")
        return jsonify({"error": "An internal server error occurred."}), 500

if __name__ == '__main__':
    # Muat model DAN kode negara saat aplikasi Flask dimulai
    if load_models() and load_country_codes(): # <-- DIUBAH: Tambahkan pemanggilan load_country_codes()
        app.run(host='0.0.0.0', port=4000, debug=True)
    else:
        print("✗ Failed to load models or country codes. Flask application will not start.")