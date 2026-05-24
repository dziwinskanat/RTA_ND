import json
import os
import sys
import time
import requests
from kafka import KafkaProducer

KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'localhost:9092')
WIKIPEDIA_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/recentchange'

print("INFO: Initializing Kafka Producer...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"ERROR: Failed to connect to Kafka broker: {e}")
    sys.exit(1)
            
print("INFO: Connecting to Wikimedia EventStreams...")
headers = {'User-Agent': 'SGH_AcademicData/1.0'}
            
try:
    while True:
        try:
            response = requests.get(WIKIPEDIA_STREAM_URL, stream=True, headers=headers, timeout=10)
            response.raise_for_status()
                    
            for line in response.iter_lines():
                if line and line.decode('utf-8').startswith('data: '):
                    try:
                        change_data = json.loads(line.decode('utf-8')[6:])
                        
                        if change_data.get('server_name') in ['en.wikipedia.org']:                             
                            producer.send('wikipedia-raw', value=change_data)
                            print(f"SENT: {change_data['title']} | Bot: {change_data['bot']}")
                    
                    except Exception: 
                        continue
        except Exception as e:
            print(f"Network error: {e}. Retrying in 5s...")
            time.sleep(5)
except KeyboardInterrupt:
    producer.close()
    print("\nINFO: Kafka Producer stopped successfully.")
