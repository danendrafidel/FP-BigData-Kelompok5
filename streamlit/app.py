import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
from minio import Minio
from datetime import datetime
import io
import time

# Initialize MinIO client
minio_client = Minio(
    "minio:9000",
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False
)

# After initializing minio_client
bucket_name = "jobs"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'jobs-topic',
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize session state
if 'jobs' not in st.session_state:
    st.session_state.jobs = []
    st.session_state.last_save_time = time.time()

def save_to_minio(data_list):
    df = pd.DataFrame(data_list)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Create a unique filename with timestamp
    filename = f"streamed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Save to MinIO
    minio_client.put_object(
        "jobs",
        filename,
        io.BytesIO(csv_buffer.getvalue().encode()),
        len(csv_buffer.getvalue())
    )
    st.success(f"Saved batch to MinIO: {filename}")

# Streamlit app
st.title('Jobs Stream Analysis')

# Create columns for metrics
col1 = st.columns(1)

# Initialize placeholder for the chart
chart_placeholder = st.empty()

# Initialize row for metrics
metrics_row = st.empty()

# Main loop
for message in consumer:
    job = message.value
    
    # Add job to session state
    st.session_state.jobs.append(job)
    
    # Check if 2 minutes have passed since last save
    current_time = time.time()
    if current_time - st.session_state.last_save_time >= 120:  # 120 seconds = 2 minutes
        if st.session_state.jobs:  # Only save if there's data
            save_to_minio(st.session_state.jobs)
            st.session_state.last_save_time = current_time
    
    # Update metrics
    with metrics_row.container():
        col1 = st.columns(1)[0]
        col1.metric("Total Jobs Streamed", len(st.session_state.jobs))

    # Update chart
    df = pd.DataFrame(st.session_state.jobs)
    # chart_placeholder.line_chart(df['vote_average'])