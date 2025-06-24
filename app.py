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
from minio import Minio  # Import MinIO client
from rapidfuzz import process, fuzz

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
country_code_map = {} # Untuk menyimpan mapping nama negara ke kode
country_names_list = []

# Inisialisasi MinIO client
minio_client = Minio(
    "localhost:9000",
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False
)
MINIO_BUCKET_NAME = "flags"
MINIO_ENDPOINT = "http://localhost:9000"

# Mapping pusat negara untuk koordinat default
country_centers = {
    'indonesia': {'lat': -2.5489, 'lon': 118.0149},
    'united states': {'lat': 39.8283, 'lon': -98.5795},
    'singapore': {'lat': 1.3521, 'lon': 103.8198},
    'malaysia': {'lat': 4.2105, 'lon': 101.9758},
    'india': {'lat': 20.5937, 'lon': 78.9629},
    'australia': {'lat': -25.2744, 'lon': 133.7751},
    'japan': {'lat': 36.2048, 'lon': 138.2529},
    'germany': {'lat': 51.1657, 'lon': 10.4515},
    'united kingdom': {'lat': 55.3781, 'lon': -3.4360},
    'canada': {'lat': 56.1304, 'lon': -106.3468},
    'china': {'lat': 35.8617, 'lon': 104.1954},
    'south korea': {'lat': 35.9078, 'lon': 127.7669},
    'thailand': {'lat': 15.8700, 'lon': 100.9925},
    'vietnam': {'lat': 14.0583, 'lon': 108.2772},
    'philippines': {'lat': 12.8797, 'lon': 121.7740},
    'taiwan': {'lat': 23.6978, 'lon': 120.9605},
    'hong kong': {'lat': 22.3193, 'lon': 114.1694},
    'netherlands': {'lat': 52.1326, 'lon': 5.2913},
    'france': {'lat': 46.2276, 'lon': 2.2137},
    'italy': {'lat': 41.8719, 'lon': 12.5674},
    'spain': {'lat': 40.4637, 'lon': -3.7492},
    'switzerland': {'lat': 46.8182, 'lon': 8.2275},
    'austria': {'lat': 47.5162, 'lon': 14.5501},
    'belgium': {'lat': 50.8503, 'lon': 4.3517},
    'denmark': {'lat': 56.2639, 'lon': 9.5018},
    'finland': {'lat': 61.9241, 'lon': 25.7482},
    'norway': {'lat': 60.4720, 'lon': 8.4689},
    'sweden': {'lat': 60.1282, 'lon': 18.6435},
    'poland': {'lat': 51.9194, 'lon': 19.1451},
    'czech republic': {'lat': 49.8175, 'lon': 15.4730},
    'slovakia': {'lat': 48.6690, 'lon': 19.6990},
    'hungary': {'lat': 47.1625, 'lon': 19.5033},
    'romania': {'lat': 45.9432, 'lon': 24.9668},
    'bulgaria': {'lat': 42.7339, 'lon': 25.4858},
    'croatia': {'lat': 45.1000, 'lon': 15.2000},
    'slovenia': {'lat': 46.0569, 'lon': 14.5058},
    'serbia': {'lat': 44.0165, 'lon': 21.0059},
    'montenegro': {'lat': 42.7087, 'lon': 19.3744},
    'bosnia and herzegovina': {'lat': 43.9159, 'lon': 17.6791},
    'macedonia': {'lat': 41.6086, 'lon': 21.7453},
    'albania': {'lat': 41.1533, 'lon': 20.1683},
    'greece': {'lat': 39.0742, 'lon': 21.8243},
    'turkey': {'lat': 38.9637, 'lon': 35.2433},
    'israel': {'lat': 31.0461, 'lon': 34.8516},
    'lebanon': {'lat': 33.8547, 'lon': 35.8623},
    'jordan': {'lat': 30.5852, 'lon': 36.2384},
    'syria': {'lat': 34.8021, 'lon': 38.9968},
    'iraq': {'lat': 33.2232, 'lon': 43.6793},
    'iran': {'lat': 32.4279, 'lon': 53.6880},
    'pakistan': {'lat': 30.3753, 'lon': 69.3451},
    'afghanistan': {'lat': 33.9391, 'lon': 67.7100},
    'bangladesh': {'lat': 23.6850, 'lon': 90.3563},
    'sri lanka': {'lat': 7.8731, 'lon': 80.7718},
    'nepal': {'lat': 28.3949, 'lon': 84.1240},
    'bhutan': {'lat': 27.5142, 'lon': 90.4336},
    'myanmar': {'lat': 21.9162, 'lon': 95.9560},
    'cambodia': {'lat': 12.5657, 'lon': 104.9910},
    'laos': {'lat': 19.8563, 'lon': 102.4955},
    'mongolia': {'lat': 46.8625, 'lon': 103.8467},
    'kazakhstan': {'lat': 48.0196, 'lon': 66.9237},
    'uzbekistan': {'lat': 41.3775, 'lon': 64.5853},
    'kyrgyzstan': {'lat': 41.2044, 'lon': 74.7661},
    'tajikistan': {'lat': 38.5358, 'lon': 71.0965},
    'turkmenistan': {'lat': 38.9697, 'lon': 59.5563},
    'azerbaijan': {'lat': 40.1431, 'lon': 47.5769},
    'georgia': {'lat': 42.3154, 'lon': 43.3569},
    'armenia': {'lat': 40.0691, 'lon': 45.0382},
    'moldova': {'lat': 47.4116, 'lon': 28.3699},
    'ukraine': {'lat': 48.3794, 'lon': 31.1656},
    'belarus': {'lat': 53.7098, 'lon': 27.9534},
    'lithuania': {'lat': 55.1694, 'lon': 23.8813},
    'latvia': {'lat': 56.8796, 'lon': 24.6032},
    'estonia': {'lat': 58.5953, 'lon': 25.0136},
    'russia': {'lat': 61.5240, 'lon': 105.3188},
    'new zealand': {'lat': -40.9006, 'lon': 174.8860},
    'south africa': {'lat': -30.5595, 'lon': 22.9375},
    'saudi arabia': {'lat': 23.8859, 'lon': 45.0792},
    'uae': {'lat': 24.0000, 'lon': 54.0000},
    'qatar': {'lat': 25.3548, 'lon': 51.1839},
    'kuwait': {'lat': 29.3117, 'lon': 47.4818},
    'oman': {'lat': 21.4735, 'lon': 55.9754},
    'yemen': {'lat': 15.5527, 'lon': 48.5164},
    'egypt': {'lat': 26.8206, 'lon': 30.8025},
    'morocco': {'lat': 31.7917, 'lon': -7.0926},
    'algeria': {'lat': 28.0339, 'lon': 1.6596},
    'tunisia': {'lat': 33.8869, 'lon': 9.5375},
    'libya': {'lat': 26.3351, 'lon': 17.2283},
    'sudan': {'lat': 12.8628, 'lon': 30.2176},
    'ethiopia': {'lat': 9.1450, 'lon': 40.4897},
    'kenya': {'lat': -0.0236, 'lon': 37.9062},
    'uganda': {'lat': 1.3733, 'lon': 32.2903},
    'tanzania': {'lat': -6.3690, 'lon': 34.8888},
    'rwanda': {'lat': -1.9403, 'lon': 29.8739},
    'burundi': {'lat': -3.3731, 'lon': 29.9189},
    'nigeria': {'lat': 9.0820, 'lon': 8.6753},
    'ghana': {'lat': 7.9465, 'lon': -1.0232},
    'cameroon': {'lat': 7.3697, 'lon': 12.3547},
    'chad': {'lat': 15.4542, 'lon': 18.7322},
    'niger': {'lat': 17.6078, 'lon': 8.0817},
    'mali': {'lat': 17.5707, 'lon': -3.9962},
    'senegal': {'lat': 14.4974, 'lon': -14.4524},
    'guinea': {'lat': 9.9456, 'lon': -9.6966},
    'sierra leone': {'lat': 8.4606, 'lon': -11.7799},
    'liberia': {'lat': 6.4281, 'lon': -9.4295},
    'cote d\'ivoire': {'lat': 7.5400, 'lon': -5.5471},
    'burkina faso': {'lat': 12.2383, 'lon': -1.5616},
    'togo': {'lat': 8.6195, 'lon': 0.8248},
    'benin': {'lat': 9.3077, 'lon': 2.3158},
    'central african republic': {'lat': 6.6111, 'lon': 20.9394},
    'congo': {'lat': -0.2280, 'lon': 15.8277},
    'democratic republic of congo': {'lat': -4.0383, 'lon': 21.7587},
    'gabon': {'lat': -0.8037, 'lon': 11.6094},
    'equatorial guinea': {'lat': 1.6508, 'lon': 10.2679},
    'sao tome and principe': {'lat': 0.1864, 'lon': 6.6131},
    'angola': {'lat': -11.2027, 'lon': 17.8739},
    'zambia': {'lat': -13.1339, 'lon': 27.8493},
    'zimbabwe': {'lat': -19.0154, 'lon': 29.1549},
    'botswana': {'lat': -22.3285, 'lon': 24.6849},
    'namibia': {'lat': -22.9576, 'lon': 18.4904},
    'lesotho': {'lat': -29.6099, 'lon': 28.2336},
    'eswatini': {'lat': -26.5225, 'lon': 31.4659},
    'mozambique': {'lat': -18.6657, 'lon': 35.5296},
    'madagascar': {'lat': -18.7669, 'lon': 46.8691},
    'comoros': {'lat': -11.6455, 'lon': 43.3333},
    'seychelles': {'lat': -4.6796, 'lon': 55.4920},
    'mauritius': {'lat': -20.3484, 'lon': 57.5522},
    'maldives': {'lat': 3.2028, 'lon': 73.2207},
    'reunion': {'lat': -21.1151, 'lon': 55.5364},
    'mayotte': {'lat': -12.8275, 'lon': 45.1662},
}

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

def load_country_codes():
    """Mengambil dan memproses kode negara dari CSV flags."""
    global country_code_map, country_names_list
    print("Loading country codes from CSV...")
    try:
        csv_path = os.path.join(os.path.dirname(__file__), "kafka", "flags.csv")
        df = pd.read_csv(csv_path)
        country_code_map = {row['Country'].lower().strip(): row['Country code'].lower().strip() for _, row in df.iterrows()}
        country_names_list = list(country_code_map.keys())
        print(f"✓ Successfully loaded {len(country_code_map)} country codes from CSV.")
        return True
    except FileNotFoundError as e:
        print(f"✗ Error: CSV file not found: {e}")
        return False
    except Exception as e:
        print(f"✗ An unexpected error occurred while processing country codes: {e}")
        return False

def get_country_code_fuzzy(country_name):
    """Get country code using fuzzy matching"""
    if not country_name:
        return None
    
    country_name = country_name.lower().strip()
    
    if not country_names_list:
        return None
    
    # Special mappings for common variations
    special_mappings = {
        'united states': 'us',
        'usa': 'us',
        'united states of america': 'us',
        'united kingdom': 'gb',
        'uk': 'gb',
        'great britain': 'gb',
        'england': 'gb',
        'netherlands': 'nl',
        'holland': 'nl',
        'russia': 'ru',
        'russian federation': 'ru',
        'south korea': 'kr',
        'korea, republic of': 'kr',
        'north korea': 'kp',
        'korea, democratic people\'s republic of': 'kp',
        'vietnam': 'vn',
        'viet nam': 'vn',
        'philippines': 'ph',
        'thailand': 'th',
        'taiwan': 'tw',
        'taiwan, province of china': 'tw',
        'hong kong': 'hk',
        'macau': 'mo',
        'macao': 'mo',
        'new zealand': 'nz',
        'south africa': 'za',
        'saudi arabia': 'sa',
        'uae': 'ae',
        'united arab emirates': 'ae',
        'switzerland': 'ch',
        'austria': 'at',
        'belgium': 'be',
        'denmark': 'dk',
        'finland': 'fi',
        'norway': 'no',
        'sweden': 'se',
        'poland': 'pl',
        'czech republic': 'cz',
        'czechia': 'cz',
        'slovakia': 'sk',
        'hungary': 'hu',
        'romania': 'ro',
        'bulgaria': 'bg',
        'croatia': 'hr',
        'slovenia': 'si',
        'serbia': 'rs',
        'montenegro': 'me',
        'bosnia and herzegovina': 'ba',
        'macedonia': 'mk',
        'albania': 'al',
        'greece': 'gr',
        'turkey': 'tr',
        'israel': 'il',
        'lebanon': 'lb',
        'jordan': 'jo',
        'syria': 'sy',
        'iraq': 'iq',
        'iran': 'ir',
        'pakistan': 'pk',
        'afghanistan': 'af',
        'bangladesh': 'bd',
        'sri lanka': 'lk',
        'nepal': 'np',
        'bhutan': 'bt',
        'myanmar': 'mm',
        'cambodia': 'kh',
        'laos': 'la',
        'mongolia': 'mn',
        'kazakhstan': 'kz',
        'uzbekistan': 'uz',
        'kyrgyzstan': 'kg',
        'tajikistan': 'tj',
        'turkmenistan': 'tm',
        'azerbaijan': 'az',
        'georgia': 'ge',
        'armenia': 'am',
        'moldova': 'md',
        'ukraine': 'ua',
        'belarus': 'by',
        'lithuania': 'lt',
        'latvia': 'lv',
        'estonia': 'ee',
        'cote d\'ivoire': 'ci',
        'burkina faso': 'bf',
        'central african republic': 'cf',
        'democratic republic of congo': 'cd',
        'equatorial guinea': 'gq',
        'sao tome and principe': 'st',
        'lesotho': 'ls',
        'eswatini': 'sz',
        'reunion': 're',
        'mayotte': 'yt',
    }
    
    # Check special mappings first
    if country_name in special_mappings:
        return special_mappings[country_name]
    
    try:
        match, score, _ = process.extractOne(country_name, country_names_list, scorer=fuzz.ratio)
        if score >= 70:  # Lowered threshold from 80 to 70
            return country_code_map[match]
        else:
            return None
    except Exception as e:
        return None

def get_flag_url_from_minio(country_code):
    """Mendapatkan URL flag dari MinIO berdasarkan country code."""
    if not country_code:
        return None
    
    try:
        # Coba berbagai ekstensi file yang mungkin ada
        extensions = ['.svg', '.png', '.jpg', '.jpeg']  # Prioritaskan SVG
        for ext in extensions:
            filename = f"{country_code.lower()}{ext}"
            try:
                # Cek apakah file ada di MinIO
                minio_client.stat_object(MINIO_BUCKET_NAME, filename)
                # Jika ada, return URL endpoint Flask untuk mengakses file
                return f"/flag/{filename}"
            except:
                continue
        
        # Jika tidak ditemukan, return None
        return None
    except Exception as e:
        print(f"Error getting flag URL for {country_code}: {e}")
        return None

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/recommend/query', methods=['POST'])
def recommend_by_query():
    """
    Endpoint untuk mendapatkan rekomendasi pekerjaan berdasarkan kueri teks.
    """
    # Tetapkan batas ambang kemiripan
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
        # Loop dan filter berdasarkan threshold
        for idx in sorted_indices:
            # 1. Hentikan jika sudah mencapai jumlah top_n
            if len(recommendations) >= top_n:
                break
            
            # 2. Hentikan jika kemiripan sudah di bawah ambang batas
            if cosine_similarities[idx] < SIMILARITY_THRESHOLD:
                break
                
            # Jika lolos kedua kondisi di atas, tambahkan ke rekomendasi
            job_info = job_data_df.iloc[idx]
            
            # Logika untuk mendapatkan flag_url dari MinIO
            DEFAULT_GLOBE_URL = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.1/icons/globe2.svg"
            country_name = job_info.get('Country', '').strip().lower()
            country_code = get_country_code_fuzzy(country_name)
            
            # Coba dapatkan flag URL dari MinIO
            flag_url = get_flag_url_from_minio(country_code) if country_code else None
            if not flag_url:
                flag_url = DEFAULT_GLOBE_URL

            # Ambil lat/lon dari mapping pusat negara
            lat = job_info.get('latitude')
            lon = job_info.get('longitude')
            
            # Jika tidak ada koordinat di data, gunakan pusat negara
            if (not lat or pd.isna(lat)) or (not lon or pd.isna(lon)):
                center = country_centers.get(country_name)
                if center:
                    lat, lon = center['lat'], center['lon']
                else:
                    lat, lon = None, None

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
                'latitude': lat,
                'longitude': lon,
                "similarity_score": round(float(cosine_similarities[idx]), 4)
            })
        
        return jsonify({"query": user_query, "recommendations": recommendations})

    except Exception as e:
        app.logger.error(f"Error in /recommend/query: {e}")
        return jsonify({"error": "An internal server error occurred."}), 500

# Endpoint untuk mengakses file flag dari MinIO
@app.route('/flag/<filename>')
def serve_flag(filename):
    """Serve flag images from MinIO"""
    try:
        # Dapatkan file dari MinIO
        response = minio_client.get_object(MINIO_BUCKET_NAME, filename)
        
        # Baca data
        data = response.read()
        
        # Tentukan content type berdasarkan ekstensi
        ext = os.path.splitext(filename)[1].lower()
        content_type = "image/png"  # default
        if ext == ".svg":
            content_type = "image/svg+xml"
        elif ext in [".jpg", ".jpeg"]:
            content_type = "image/jpeg"
        
        # Tutup response
        response.close()
        response.release_conn()
        
        # Return file dengan content type yang benar
        from flask import Response
        return Response(data, content_type=content_type)
        
    except Exception as e:
        # Return default globe icon jika file tidak ditemukan
        return Response(
            '<svg width="40" height="30" xmlns="http://www.w3.org/2000/svg"><circle cx="20" cy="15" r="10" fill="#f0f0f0" stroke="#ccc"/></svg>',
            content_type="image/svg+xml"
        )

if __name__ == '__main__':
    # Muat model DAN kode negara saat aplikasi Flask dimulai
    if load_models() and load_country_codes():
        app.run(host='0.0.0.0', port=4000, debug=True)
    else:
        print("✗ Failed to load models or country codes. Flask application will not start.")