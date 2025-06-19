#!/usr/bin/env python3
from flask import Flask, request, jsonify
from flask import render_template
import pickle
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer # Hanya untuk type hinting, tidak perlu jika hanya load
from sklearn.metrics.pairwise import cosine_similarity
import re
import os

app = Flask(__name__)

# --- Path ke file model ---
MODEL_DIR = './models_tfidf/' # Pastikan ini sesuai dengan tempat Anda menyimpan model
TFIDF_VECTORIZER_FILE = os.path.join(MODEL_DIR, 'tfidf_vectorizer.pkl')
TFIDF_MATRIX_FILE = os.path.join(MODEL_DIR, 'tfidf_matrix.pkl')
JOB_DATA_TFIDF_FILE = os.path.join(MODEL_DIR, 'job_data_tfidf.pkl')
COSINE_SIM_MATRIX_FILE = os.path.join(MODEL_DIR, 'cosine_similarity_matrix.pkl') # Untuk rekomendasi by ID

# --- Variabel global untuk menyimpan model yang dimuat ---
vectorizer = None
tfidf_matrix_jobs = None # Matriks TF-IDF untuk semua pekerjaan
job_data_df = None
cosine_sim_matrix_jobs = None # Matriks kemiripan antar pekerjaan

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

        if os.path.exists(COSINE_SIM_MATRIX_FILE):
            with open(COSINE_SIM_MATRIX_FILE, 'rb') as f:
                cosine_sim_matrix_jobs = pickle.load(f)
            print(f"✓ Cosine similarity matrix loaded from {COSINE_SIM_MATRIX_FILE}")
        else:
            print(f"Warning: Cosine similarity matrix file not found at {COSINE_SIM_MATRIX_FILE}. Recommendation by job_id might be slower or unavailable if not computed on the fly.")
            # Jika diperlukan, Anda bisa menghitungnya di sini jika tfidf_matrix_jobs ada
            # if tfidf_matrix_jobs is not None:
            #    print("Calculating cosine similarity matrix on load...")
            #    cosine_sim_matrix_jobs = cosine_similarity(tfidf_matrix_jobs)

        print("✓ All models and data loaded successfully.")
        return True
    except FileNotFoundError as e:
        print(f"✗ Error: Model file not found: {e}. Please ensure models are trained and saved correctly.")
        return False
    except Exception as e:
        print(f"✗ An unexpected error occurred while loading models: {e}")
        return False

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/recommend/query', methods=['POST'])
def recommend_by_query():
    """
    Endpoint untuk mendapatkan rekomendasi pekerjaan berdasarkan kueri teks.
    Input JSON: {"query": "teks kueri pengguna", "top_n": 5}
    """
    if vectorizer is None or tfidf_matrix_jobs is None or job_data_df is None:
        return jsonify({"error": "Models not loaded. Please try again later or check server logs."}), 500

    try:
        data = request.get_json()
        user_query = data.get('query')
        top_n = data.get('top_n', 5) # Default ke 5 jika tidak disediakan

        if not user_query:
            return jsonify({"error": "Query parameter is missing."}), 400
        if not isinstance(top_n, int) or top_n <= 0:
            return jsonify({"error": "top_n parameter must be a positive integer."}), 400

        cleaned_query = _clean_text(user_query)
        query_vector = vectorizer.transform([cleaned_query])
        
        cosine_similarities = cosine_similarity(query_vector, tfidf_matrix_jobs).flatten()
        top_n_indices = np.argsort(cosine_similarities)[::-1][:top_n]
        
        recommendations = []
        print(job_data_df.columns) # Debugging: Cek kolom yang tersedia
        for idx in top_n_indices:
            job_info = job_data_df.iloc[idx]
            recommendations.append({
                "job_id": str(job_info['Job Id']), # Konversi ke string jika numerik untuk konsistensi JSON
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
                "similarity_score": round(float(cosine_similarities[idx]), 4)
            })
        
        return jsonify({"query": user_query, "recommendations": recommendations})

    except Exception as e:
        app.logger.error(f"Error in /recommend/query: {e}")
        return jsonify({"error": "An internal server error occurred."}), 500

if __name__ == '__main__':
    # Muat model saat aplikasi Flask dimulai
    if load_models():
        # Jalankan aplikasi Flask
        # Gunakan host='0.0.0.0' agar bisa diakses dari luar container/mesin lain jika perlu
        app.run(host='0.0.0.0', port=4000, debug=True) # debug=True hanya untuk pengembangan
    else:
        print("✗ Failed to load models. Flask application will not start.")