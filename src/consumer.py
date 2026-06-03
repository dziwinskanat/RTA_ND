from kafka import KafkaConsumer
from pathlib import Path
from datetime import datetime, timezone
import json, os, time, pandas as pd
 
KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES", 100_000))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 5_000))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "./data"))
OUTPUT_FMT = os.environ.get("OUTPUT_FORMAT", "csv") 
TOPIC = "wikipedia-raw"
 
 
def flatten(raw: dict) -> dict:
    length = raw.get("length") or {}
    revision = raw.get("revision") or {}
    return {
        "id":                 raw.get("id"),
        "type":               raw.get("type"),
        "namespace":          raw.get("namespace"),
        "title":              raw.get("title"),
        "comment":            raw.get("comment"),
        "parsedcomment":      raw.get("parsedcomment"),
        "timestamp":          raw.get("timestamp"),
        "user":               raw.get("user"),
        "bot":                raw.get("bot"),
        "minor":              raw.get("minor"),        # opcjonalne – None dla logów
        "patrolled":          raw.get("patrolled"),    # opcjonalne – None dla logów
        "server_name":        raw.get("server_name"),
        "wiki":               raw.get("wiki"),
        "length_old":         length.get("old"),       # None dla eventów bez length
        "length_new":         length.get("new"),
        "revision_old":       revision.get("old"),     # None dla eventów bez revision
        "revision_new":       revision.get("new"),
    }
 
 
def save_batch(records: list, batch_idx: int) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    path = OUTPUT_DIR / f"wikipedia_raw_{batch_idx:04d}_{ts}.{OUTPUT_FMT}"
    df = pd.DataFrame(records)
    df.to_parquet(path, index=False) if OUTPUT_FMT == "parquet" else df.to_csv(path, index=False)
    print(f"Zapisano {len(records)} rekordów → {path}")
 
 
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[os.environ.get("KAFKA_SERVER", "localhost:9092")],
    auto_offset_reset="earliest",
    group_id=None,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    consumer_timeout_ms=30_000,
)
 
buffer, total, batch_idx, t0 = [], 0, 0, time.time()
 
print(f"Zbieranie danych z '{TOPIC}' → {OUTPUT_DIR} ({OUTPUT_FMT}), limit: {MAX_MESSAGES}")
 
try:
    for msg in consumer:
        try:
            buffer.append(flatten(msg.value))
            total += 1
        except Exception:
            continue
 
        if len(buffer) >= BATCH_SIZE:
            save_batch(buffer, batch_idx)
            batch_idx += 1
            buffer = []
 
        if total % 1000 == 0:
            print(f"  {total}/{MAX_MESSAGES} ({total/(time.time()-t0):.0f} msg/s)")
 
        if total >= MAX_MESSAGES:
            print(f"Osiągnięto limit {MAX_MESSAGES}.")
            break
 
except KeyboardInterrupt:
    print("Przerwano.")
finally:
    if buffer:
        save_batch(buffer, batch_idx)
    consumer.close()
    print(f"Gotowe: {total} rekordów w {time.time()-t0:.1f}s")