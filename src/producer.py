import json
import sys
import time
import requests
from kafka import KafkaProducer

KAFKA_SERVER = 'localhost:9092'
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
                        
                        if change_data.get('server_name') in ['pl.wikipedia.org', 'en.wikipedia.org']:
                            
                            filtered_data = {
                                "id": change_data.get("id"),
                                "title": change_data.get("title"),
                                "user": change_data.get("user"),
                                "bot": change_data.get("bot", False),
                                "timestamp": change_data.get("timestamp")
                            }
                            
                            producer.send('wikipedia-raw', value=filtered_data)
                            
                            prediction_data = {
                                "id": filtered_data["id"],
                                "is_bot": filtered_data["bot"]
                            }
                            producer.send('predictions-stream', value=prediction_data)
                            
                            print(f"SENT: {filtered_data['title']} | Bot: {filtered_data['bot']}")
                    
                    except Exception: 
                        continue
        except Exception as e:
            print(f"Network error: {e}. Retrying in 5s...")
            time.sleep(5)
except KeyboardInterrupt:
    producer.close()
    print("\nINFO: Kafka Producer stopped successfully.")
