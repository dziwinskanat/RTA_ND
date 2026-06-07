import json
import os
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from xgboost import XGBClassifier

from feature_engineering import WikipediaFeatureEngineer


KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "wikipedia-raw")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "predictions-stream")
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/xgb_bot_detector.json")


def load_model():
    if not os.path.exists(MODEL_PATH):
        print(f"ERROR: Nie znaleziono modelu: {MODEL_PATH}")
        sys.exit(1)

    model = XGBClassifier()
    model.load_model(MODEL_PATH)

    print(f"INFO: Wczytano model XGBoost: {MODEL_PATH}")
    return model


def main():
    print("INFO: Start live scoring consumer")

    model = load_model()
    model_features = model.get_booster().feature_names
    feature_engineer = WikipediaFeatureEngineer()

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset="earliest",
        group_id=None,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"INFO: Czytam z topicu: {INPUT_TOPIC}")
    print(f"INFO: Wyniki wysyłam do: {OUTPUT_TOPIC}")

    try:
        for message in consumer:
            event = message.value

            try:
                print(
                    f"INFO: Otrzymano event: "
                    f"{event.get('title', 'BRAK_TYTUŁU')}"
                )

                X = feature_engineer.transform_record(event)

                if model_features is not None:
                    X = X.reindex(columns=model_features, fill_value=0)

                probability = float(model.predict_proba(X)[0][1])
                predicted_bot = bool(probability >= 0.5)

                result = {
                    "scored_at": datetime.now(timezone.utc).isoformat(),
                    "timestamp": event.get("timestamp"),
                    "event_id": event.get("id"),
                    "title": event.get("title"),
                    "user": event.get("user"),
                    "wiki": event.get("wiki"),
                    "actual_bot": event.get("bot"),
                    "predicted_bot": predicted_bot,
                    "probability": probability,
                }

                producer.send(OUTPUT_TOPIC, value=result)
                producer.flush()

                print(
                    f"PREDICTION | "
                    f"actual={result['actual_bot']} | "
                    f"predicted={result['predicted_bot']} | "
                    f"probability={result['probability']:.4f} | "
                    f"title={result['title']}"
                )

            except Exception as e:
                print(f"ERROR: Błąd przy predykcji: {e}")

    except KeyboardInterrupt:
        print("INFO: Zatrzymano consumer.")

    finally:
        consumer.close()
        producer.close()
        print("INFO: Zamknięto połączenia.")


if __name__ == "__main__":
    main()
