#!/usr/bin/env python3
"""
Script sederhana untuk setup flag dataset dari folder data/flags/ dan upload ke MinIO
"""

import pandas as pd
import requests
from minio import Minio
import os
import io
import time

def setup_flags():
    """Setup flag dataset dari folder data/flags/ dan upload ke MinIO"""
    
    # Inisialisasi MinIO client
    minio_client = Minio(
        "localhost:9000",
        access_key="minio_access_key",
        secret_key="minio_secret_key",
        secure=False
    )
    
    bucket_name = "flags"
    
    # Buat bucket jika belum ada
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' dibuat")
    else:
        print(f"✅ Bucket '{bucket_name}' sudah ada")
    
    # Dataset flag sederhana - menggunakan flagcdn.com
    flags_data = [
        {"Country": "Indonesia", "Country code": "id", "Flag URL": "https://flagcdn.com/id.svg"},
        {"Country": "United States", "Country code": "us", "Flag URL": "https://flagcdn.com/us.svg"},
        {"Country": "Singapore", "Country code": "sg", "Flag URL": "https://flagcdn.com/sg.svg"},
        {"Country": "Malaysia", "Country code": "my", "Flag URL": "https://flagcdn.com/my.svg"},
        {"Country": "India", "Country code": "in", "Flag URL": "https://flagcdn.com/in.svg"},
        {"Country": "Australia", "Country code": "au", "Flag URL": "https://flagcdn.com/au.svg"},
        {"Country": "Japan", "Country code": "jp", "Flag URL": "https://flagcdn.com/jp.svg"},
        {"Country": "Germany", "Country code": "de", "Flag URL": "https://flagcdn.com/de.svg"},
        {"Country": "United Kingdom", "Country code": "gb", "Flag URL": "https://flagcdn.com/gb.svg"},
        {"Country": "Canada", "Country code": "ca", "Flag URL": "https://flagcdn.com/ca.svg"},
        {"Country": "China", "Country code": "cn", "Flag URL": "https://flagcdn.com/cn.svg"},
        {"Country": "South Korea", "Country code": "kr", "Flag URL": "https://flagcdn.com/kr.svg"},
        {"Country": "Thailand", "Country code": "th", "Flag URL": "https://flagcdn.com/th.svg"},
        {"Country": "Vietnam", "Country code": "vn", "Flag URL": "https://flagcdn.com/vn.svg"},
        {"Country": "Philippines", "Country code": "ph", "Flag URL": "https://flagcdn.com/ph.svg"},
        {"Country": "Taiwan", "Country code": "tw", "Flag URL": "https://flagcdn.com/tw.svg"},
        {"Country": "Hong Kong", "Country code": "hk", "Flag URL": "https://flagcdn.com/hk.svg"},
        {"Country": "Netherlands", "Country code": "nl", "Flag URL": "https://flagcdn.com/nl.svg"},
        {"Country": "France", "Country code": "fr", "Flag URL": "https://flagcdn.com/fr.svg"},
        {"Country": "Italy", "Country code": "it", "Flag URL": "https://flagcdn.com/it.svg"},
        {"Country": "Spain", "Country code": "es", "Flag URL": "https://flagcdn.com/es.svg"},
        {"Country": "Switzerland", "Country code": "ch", "Flag URL": "https://flagcdn.com/ch.svg"},
        {"Country": "Austria", "Country code": "at", "Flag URL": "https://flagcdn.com/at.svg"},
        {"Country": "Belgium", "Country code": "be", "Flag URL": "https://flagcdn.com/be.svg"},
        {"Country": "Denmark", "Country code": "dk", "Flag URL": "https://flagcdn.com/dk.svg"},
        {"Country": "Finland", "Country code": "fi", "Flag URL": "https://flagcdn.com/fi.svg"},
        {"Country": "Norway", "Country code": "no", "Flag URL": "https://flagcdn.com/no.svg"},
        {"Country": "Sweden", "Country code": "se", "Flag URL": "https://flagcdn.com/se.svg"},
        {"Country": "Poland", "Country code": "pl", "Flag URL": "https://flagcdn.com/pl.svg"},
        {"Country": "Czech Republic", "Country code": "cz", "Flag URL": "https://flagcdn.com/cz.svg"},
        {"Country": "Slovakia", "Country code": "sk", "Flag URL": "https://flagcdn.com/sk.svg"},
        {"Country": "Hungary", "Country code": "hu", "Flag URL": "https://flagcdn.com/hu.svg"},
        {"Country": "Romania", "Country code": "ro", "Flag URL": "https://flagcdn.com/ro.svg"},
        {"Country": "Bulgaria", "Country code": "bg", "Flag URL": "https://flagcdn.com/bg.svg"},
        {"Country": "Croatia", "Country code": "hr", "Flag URL": "https://flagcdn.com/hr.svg"},
        {"Country": "Slovenia", "Country code": "si", "Flag URL": "https://flagcdn.com/si.svg"},
        {"Country": "Serbia", "Country code": "rs", "Flag URL": "https://flagcdn.com/rs.svg"},
        {"Country": "Montenegro", "Country code": "me", "Flag URL": "https://flagcdn.com/me.svg"},
        {"Country": "Bosnia and Herzegovina", "Country code": "ba", "Flag URL": "https://flagcdn.com/ba.svg"},
        {"Country": "Macedonia", "Country code": "mk", "Flag URL": "https://flagcdn.com/mk.svg"},
        {"Country": "Albania", "Country code": "al", "Flag URL": "https://flagcdn.com/al.svg"},
        {"Country": "Greece", "Country code": "gr", "Flag URL": "https://flagcdn.com/gr.svg"},
        {"Country": "Turkey", "Country code": "tr", "Flag URL": "https://flagcdn.com/tr.svg"},
        {"Country": "Israel", "Country code": "il", "Flag URL": "https://flagcdn.com/il.svg"},
        {"Country": "Lebanon", "Country code": "lb", "Flag URL": "https://flagcdn.com/lb.svg"},
        {"Country": "Jordan", "Country code": "jo", "Flag URL": "https://flagcdn.com/jo.svg"},
        {"Country": "Syria", "Country code": "sy", "Flag URL": "https://flagcdn.com/sy.svg"},
        {"Country": "Iraq", "Country code": "iq", "Flag URL": "https://flagcdn.com/iq.svg"},
        {"Country": "Iran", "Country code": "ir", "Flag URL": "https://flagcdn.com/ir.svg"},
        {"Country": "Pakistan", "Country code": "pk", "Flag URL": "https://flagcdn.com/pk.svg"},
        {"Country": "Afghanistan", "Country code": "af", "Flag URL": "https://flagcdn.com/af.svg"},
        {"Country": "Bangladesh", "Country code": "bd", "Flag URL": "https://flagcdn.com/bd.svg"},
        {"Country": "Sri Lanka", "Country code": "lk", "Flag URL": "https://flagcdn.com/lk.svg"},
        {"Country": "Nepal", "Country code": "np", "Flag URL": "https://flagcdn.com/np.svg"},
        {"Country": "Bhutan", "Country code": "bt", "Flag URL": "https://flagcdn.com/bt.svg"},
        {"Country": "Myanmar", "Country code": "mm", "Flag URL": "https://flagcdn.com/mm.svg"},
        {"Country": "Cambodia", "Country code": "kh", "Flag URL": "https://flagcdn.com/kh.svg"},
        {"Country": "Laos", "Country code": "la", "Flag URL": "https://flagcdn.com/la.svg"},
        {"Country": "Mongolia", "Country code": "mn", "Flag URL": "https://flagcdn.com/mn.svg"},
        {"Country": "Kazakhstan", "Country code": "kz", "Flag URL": "https://flagcdn.com/kz.svg"},
        {"Country": "Uzbekistan", "Country code": "uz", "Flag URL": "https://flagcdn.com/uz.svg"},
        {"Country": "Kyrgyzstan", "Country code": "kg", "Flag URL": "https://flagcdn.com/kg.svg"},
        {"Country": "Tajikistan", "Country code": "tj", "Flag URL": "https://flagcdn.com/tj.svg"},
        {"Country": "Turkmenistan", "Country code": "tm", "Flag URL": "https://flagcdn.com/tm.svg"},
        {"Country": "Azerbaijan", "Country code": "az", "Flag URL": "https://flagcdn.com/az.svg"},
        {"Country": "Georgia", "Country code": "ge", "Flag URL": "https://flagcdn.com/ge.svg"},
        {"Country": "Armenia", "Country code": "am", "Flag URL": "https://flagcdn.com/am.svg"},
        {"Country": "Moldova", "Country code": "md", "Flag URL": "https://flagcdn.com/md.svg"},
        {"Country": "Ukraine", "Country code": "ua", "Flag URL": "https://flagcdn.com/ua.svg"},
        {"Country": "Belarus", "Country code": "by", "Flag URL": "https://flagcdn.com/by.svg"},
        {"Country": "Lithuania", "Country code": "lt", "Flag URL": "https://flagcdn.com/lt.svg"},
        {"Country": "Latvia", "Country code": "lv", "Flag URL": "https://flagcdn.com/lv.svg"},
        {"Country": "Estonia", "Country code": "ee", "Flag URL": "https://flagcdn.com/ee.svg"},
        {"Country": "Russia", "Country code": "ru", "Flag URL": "https://flagcdn.com/ru.svg"},
        {"Country": "New Zealand", "Country code": "nz", "Flag URL": "https://flagcdn.com/nz.svg"},
        {"Country": "South Africa", "Country code": "za", "Flag URL": "https://flagcdn.com/za.svg"},
        {"Country": "Saudi Arabia", "Country code": "sa", "Flag URL": "https://flagcdn.com/sa.svg"},
        {"Country": "UAE", "Country code": "ae", "Flag URL": "https://flagcdn.com/ae.svg"},
        {"Country": "Qatar", "Country code": "qa", "Flag URL": "https://flagcdn.com/qa.svg"},
        {"Country": "Kuwait", "Country code": "kw", "Flag URL": "https://flagcdn.com/kw.svg"},
        {"Country": "Oman", "Country code": "om", "Flag URL": "https://flagcdn.com/om.svg"},
        {"Country": "Yemen", "Country code": "ye", "Flag URL": "https://flagcdn.com/ye.svg"},
        {"Country": "Egypt", "Country code": "eg", "Flag URL": "https://flagcdn.com/eg.svg"},
        {"Country": "Morocco", "Country code": "ma", "Flag URL": "https://flagcdn.com/ma.svg"},
        {"Country": "Algeria", "Country code": "dz", "Flag URL": "https://flagcdn.com/dz.svg"},
        {"Country": "Tunisia", "Country code": "tn", "Flag URL": "https://flagcdn.com/tn.svg"},
        {"Country": "Libya", "Country code": "ly", "Flag URL": "https://flagcdn.com/ly.svg"},
        {"Country": "Sudan", "Country code": "sd", "Flag URL": "https://flagcdn.com/sd.svg"},
        {"Country": "Ethiopia", "Country code": "et", "Flag URL": "https://flagcdn.com/et.svg"},
        {"Country": "Kenya", "Country code": "ke", "Flag URL": "https://flagcdn.com/ke.svg"},
        {"Country": "Uganda", "Country code": "ug", "Flag URL": "https://flagcdn.com/ug.svg"},
        {"Country": "Tanzania", "Country code": "tz", "Flag URL": "https://flagcdn.com/tz.svg"},
        {"Country": "Rwanda", "Country code": "rw", "Flag URL": "https://flagcdn.com/rw.svg"},
        {"Country": "Burundi", "Country code": "bi", "Flag URL": "https://flagcdn.com/bi.svg"},
        {"Country": "Nigeria", "Country code": "ng", "Flag URL": "https://flagcdn.com/ng.svg"},
        {"Country": "Ghana", "Country code": "gh", "Flag URL": "https://flagcdn.com/gh.svg"},
        {"Country": "Cameroon", "Country code": "cm", "Flag URL": "https://flagcdn.com/cm.svg"},
        {"Country": "Chad", "Country code": "td", "Flag URL": "https://flagcdn.com/td.svg"},
        {"Country": "Niger", "Country code": "ne", "Flag URL": "https://flagcdn.com/ne.svg"},
        {"Country": "Mali", "Country code": "ml", "Flag URL": "https://flagcdn.com/ml.svg"},
        {"Country": "Senegal", "Country code": "sn", "Flag URL": "https://flagcdn.com/sn.svg"},
        {"Country": "Guinea", "Country code": "gn", "Flag URL": "https://flagcdn.com/gn.svg"},
        {"Country": "Sierra Leone", "Country code": "sl", "Flag URL": "https://flagcdn.com/sl.svg"},
        {"Country": "Liberia", "Country code": "lr", "Flag URL": "https://flagcdn.com/lr.svg"},
        {"Country": "Cote d'Ivoire", "Country code": "ci", "Flag URL": "https://flagcdn.com/ci.svg"},
        {"Country": "Burkina Faso", "Country code": "bf", "Flag URL": "https://flagcdn.com/bf.svg"},
        {"Country": "Togo", "Country code": "tg", "Flag URL": "https://flagcdn.com/tg.svg"},
        {"Country": "Benin", "Country code": "bj", "Flag URL": "https://flagcdn.com/bj.svg"},
        {"Country": "Central African Republic", "Country code": "cf", "Flag URL": "https://flagcdn.com/cf.svg"},
        {"Country": "Congo", "Country code": "cg", "Flag URL": "https://flagcdn.com/cg.svg"},
        {"Country": "Democratic Republic of Congo", "Country code": "cd", "Flag URL": "https://flagcdn.com/cd.svg"},
        {"Country": "Gabon", "Country code": "ga", "Flag URL": "https://flagcdn.com/ga.svg"},
        {"Country": "Equatorial Guinea", "Country code": "gq", "Flag URL": "https://flagcdn.com/gq.svg"},
        {"Country": "Sao Tome and Principe", "Country code": "st", "Flag URL": "https://flagcdn.com/st.svg"},
        {"Country": "Angola", "Country code": "ao", "Flag URL": "https://flagcdn.com/ao.svg"},
        {"Country": "Zambia", "Country code": "zm", "Flag URL": "https://flagcdn.com/zm.svg"},
        {"Country": "Zimbabwe", "Country code": "zw", "Flag URL": "https://flagcdn.com/zw.svg"},
        {"Country": "Botswana", "Country code": "bw", "Flag URL": "https://flagcdn.com/bw.svg"},
        {"Country": "Namibia", "Country code": "na", "Flag URL": "https://flagcdn.com/na.svg"},
        {"Country": "Lesotho", "Country code": "ls", "Flag URL": "https://flagcdn.com/ls.svg"},
        {"Country": "Eswatini", "Country code": "sz", "Flag URL": "https://flagcdn.com/sz.svg"},
        {"Country": "Mozambique", "Country code": "mz", "Flag URL": "https://flagcdn.com/mz.svg"},
        {"Country": "Madagascar", "Country code": "mg", "Flag URL": "https://flagcdn.com/mg.svg"},
        {"Country": "Comoros", "Country code": "km", "Flag URL": "https://flagcdn.com/km.svg"},
        {"Country": "Seychelles", "Country code": "sc", "Flag URL": "https://flagcdn.com/sc.svg"},
        {"Country": "Mauritius", "Country code": "mu", "Flag URL": "https://flagcdn.com/mu.svg"},
        {"Country": "Maldives", "Country code": "mv", "Flag URL": "https://flagcdn.com/mv.svg"},
        {"Country": "Reunion", "Country code": "re", "Flag URL": "https://flagcdn.com/re.svg"},
        {"Country": "Mayotte", "Country code": "yt", "Flag URL": "https://flagcdn.com/yt.svg"},
    ]
    
    df = pd.DataFrame(flags_data)
    print(f"📊 Dataset flag: {len(df)} negara")
    
    # Upload flags ke MinIO
    success_count = 0
    failed_count = 0
    
    print("📤 Uploading flags ke MinIO...")
    
    for idx, row in df.iterrows():
        country = row['Country']
        country_code = row['Country code']
        flag_url = row['Flag URL']
        
        filename = f"{country_code.lower()}.svg"
        
        try:
            # Download flag dari flagcdn.com
            response = requests.get(flag_url, timeout=10)
            if response.status_code == 200:
                data = response.content
                
                # Upload ke MinIO
                minio_client.put_object(
                    bucket_name,
                    filename,
                    data=io.BytesIO(data),
                    length=len(data),
                    content_type="image/svg+xml"
                )
                success_count += 1
                print(f"✅ {country} ({country_code})")
            else:
                failed_count += 1
                print(f"❌ {country} - HTTP {response.status_code}")
                
        except Exception as e:
            failed_count += 1
            print(f"❌ {country} - {str(e)[:50]}")
    
    # Simpan CSV untuk app.py
    df['Flag'] = df['Flag URL']  # Tambahkan kolom Flag untuk kompatibilitas
    df['Region'] = 'EMEA'  # Default region
    
    csv_path = "kafka/flags.csv"
    df.to_csv(csv_path, index=False)
    
    print(f"\n🎉 Setup selesai!")
    print(f"✅ Success: {success_count} flags")
    print(f"❌ Failed: {failed_count} flags")
    print(f"📁 CSV saved: {csv_path}")
    print(f"🌐 Test dengan: python test_flags.py")
    print(f"\n📋 Cara pakai:")
    print(f"   1. python setup_flags.py (sudah dijalankan)")
    print(f"   2. python app.py (jalankan Flask app)")
    print(f"   3. python test_flags.py (test flag system)")

if __name__ == "__main__":
    setup_flags() 