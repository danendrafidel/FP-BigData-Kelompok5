from kafka import KafkaProducer
import json
import time
import pandas as pd
import os

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

# Get the current script's directory and read the CSV file with correct path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
csv_path = os.path.join(parent_dir, 'kafka', 'dataset.csv')

# Print the path being used
print(f"Attempting to read CSV from: {csv_path}")

# Read the CSV file
df = pd.read_csv(csv_path)

# Keep track of processed records in memory
processed_records = set()
total_processed = 0
total_skipped = 0

# Stream each row with duplicate prevention and counting
def stream_data(start_index=0):
    global total_processed, total_skipped

    # Print available columns
    print("Available columns in CSV:", df.columns.tolist())
    print(f"Total records in CSV: {len(df)}")
    print(f"Starting from row index: {start_index}")
    
    for index, row in df.iloc[start_index:].iterrows():
        data = row.to_dict()
        
        # Create a unique identifier using just the title
        record_id = str(data.get('Job Title', ''))

        producer.send('jobs-topic', data)
        processed_records.add(record_id)
        total_processed += 1
        print(f"Sent ({index}): {data['Job Title']}")
        time.sleep(0.01)
    
    print("\nFinal Statistics:")
    print(f"Total records processed: {total_processed}")
    print(f"Total duplicates skipped: {total_skipped}")
    print(f"Total records handled: {total_processed + total_skipped}")

if __name__ == "__main__":
    try:
        stream_data()
    except FileNotFoundError:
        print(f"Error: Could not find dataset.csv at {csv_path}")
        print(f"Current working directory: {os.getcwd()}")
    except Exception as e:
        print(f"Error occurred: {str(e)}")