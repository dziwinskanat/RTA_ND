import os
import json
import time
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer, TopicPartition


KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")
PREDICTIONS_TOPIC = os.environ.get("PREDICTIONS_TOPIC", "predictions-stream")
CONSUMER_GROUP_ID = os.environ.get("CONSUMER_GROUP_ID", "streamlit-dashboard-v3")


st.set_page_config(
    page_title="Wikipedia Bot Detection Dashboard",
    page_icon="🤖",
    layout="wide"
)


def parse_bool(value):
    """
    Zamienia różne reprezentacje wartości logicznych na True/False.
    Obsługuje bool, stringi oraz wartości brakujące.
    """
    if isinstance(value, bool):
        return value

    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip().lower()
        if value in ["true", "1", "yes", "y"]:
            return True
        if value in ["false", "0", "no", "n"]:
            return False

    return None


@st.cache_resource
def get_consumer():
    """
    Tworzy KafkaConsumer dla dashboardu.

    Używamy ręcznego przypisania partycji zamiast consumer group,
    żeby uniknąć problemów z zapamiętanymi offsetami podczas testów lokalnych.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_SERVER],
        enable_auto_commit=False,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
    )

    partitions = None

    for _ in range(10):
        partitions = consumer.partitions_for_topic(PREDICTIONS_TOPIC)
        if partitions:
            break
        time.sleep(1)

    if not partitions:
        raise RuntimeError(
            f"Nie znaleziono partycji dla topica {PREDICTIONS_TOPIC}. "
            "Sprawdź, czy topic istnieje i Kafka działa."
        )

    topic_partitions = [
        TopicPartition(PREDICTIONS_TOPIC, partition)
        for partition in partitions
    ]

    consumer.assign(topic_partitions)

    # Na potrzeby testów i prezentacji czytamy od początku topica,
    # żeby dashboard od razu miał dane po uruchomieniu.
    consumer.seek_to_beginning(*topic_partitions)

    return consumer


def read_new_messages(consumer, max_records=500, timeout_ms=1000):
    """
    Pobiera nowe wiadomości z topica predictions-stream.
    """
    records = []

    messages = consumer.poll(
        timeout_ms=timeout_ms,
        max_records=max_records
    )

    for _, partition_messages in messages.items():
        for message in partition_messages:
            record = message.value

            if isinstance(record, dict):
                record["_kafka_offset"] = message.offset
                record["_kafka_partition"] = message.partition
                records.append(record)


    return records


def prepare_dataframe(events):
    """
    Przygotowuje dane do agregacji i wizualizacji.
    """
    if not events:
        return pd.DataFrame()

    df = pd.DataFrame(events)

    if "scored_at" in df.columns:
        df["scored_at_dt"] = pd.to_datetime(
            df["scored_at"],
            errors="coerce",
            utc=True
        )
    else:
        df["scored_at_dt"] = pd.NaT

    df["scored_at_dt"] = df["scored_at_dt"].fillna(
        pd.Timestamp.now(tz="UTC")
    )

    df["predicted_bot_bool"] = df["predicted_bot"].apply(parse_bool)
    df["actual_bot_bool"] = df["actual_bot"].apply(parse_bool)

    df["predicted_label"] = df["predicted_bot_bool"].map(
        {True: "Bot", False: "Human"}
    ).fillna("Unknown")

    df["actual_label"] = df["actual_bot_bool"].map(
        {True: "Bot", False: "Human"}
    ).fillna("Unknown")

    df["probability"] = pd.to_numeric(
        df.get("probability"),
        errors="coerce"
    )

    df["false_alarm"] = (
        (df["predicted_bot_bool"] == True)
        & (df["actual_bot_bool"] == False)
    )

    return df


# -----------------------
# Sidebar
# -----------------------

st.sidebar.title("Ustawienia dashboardu")

window_minutes = st.sidebar.slider(
    "Ruchome okno czasowe, minuty",
    min_value=1,
    max_value=30,
    value=5,
    step=1
)

max_stored_events = st.sidebar.slider(
    "Maksymalna liczba eventów w pamięci",
    min_value=500,
    max_value=10000,
    value=3000,
    step=500
)

refresh_seconds = st.sidebar.slider(
    "Odświeżanie, sekundy",
    min_value=1,
    max_value=10,
    value=3,
    step=1
)

auto_refresh = st.sidebar.checkbox(
    "Automatyczne odświeżanie",
    value=True
)


# -----------------------
# Session state
# -----------------------

if "events" not in st.session_state:
    st.session_state["events"] = []


# -----------------------
# Kafka read
# -----------------------

st.title("🤖 Real-time Wikipedia Bot Detection Dashboard")

st.caption(
    f"Źródło danych: Kafka topic `{PREDICTIONS_TOPIC}` | "
    f"Broker: `{KAFKA_SERVER}`"
)

try:
    consumer = get_consumer()
    new_events = read_new_messages(consumer)

    if new_events:
        st.session_state["events"].extend(new_events)
        st.session_state["events"] = st.session_state["events"][-max_stored_events:]

except Exception as error:
    st.error(f"Błąd połączenia z Kafką: {error}")
    st.stop()


df = prepare_dataframe(st.session_state["events"])

if df.empty:
    st.info(
        "Brak danych do wyświetlenia. "
        "Upewnij się, że działa producer oraz live scoring consumer."
    )

    if auto_refresh:
        time.sleep(refresh_seconds)
        st.rerun()

    st.stop()


# -----------------------
# Window filtering
# -----------------------

now = pd.Timestamp.now(tz="UTC")
window_start = now - pd.Timedelta(minutes=window_minutes)

window_df = df[df["scored_at_dt"] >= window_start].copy()


# -----------------------
# Metrics
# -----------------------

total_events = len(window_df)
predicted_bots = int((window_df["predicted_bot_bool"] == True).sum())
predicted_humans = int((window_df["predicted_bot_bool"] == False).sum())
false_alarms = int(window_df["false_alarm"].sum())

avg_probability = window_df["probability"].mean()
avg_probability_display = (
    f"{avg_probability:.3f}" if pd.notna(avg_probability) else "brak danych"
)

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric(
    label=f"Edycje w ostatnich {window_minutes} min",
    value=total_events
)

col2.metric(
    label="Przewidziane boty",
    value=predicted_bots
)

col3.metric(
    label="Przewidziani ludzie",
    value=predicted_humans
)

col4.metric(
    label="Fałszywe alarmy",
    value=false_alarms
)

col5.metric(
    label="Śr. prawdopodobieństwo bota",
    value=avg_probability_display
)


# -----------------------
# Charts
# -----------------------

st.subheader("Liczba przewidzianych botów vs ludzi w czasie")

if not window_df.empty:
    window_df["minute"] = window_df["scored_at_dt"].dt.floor("min")

    bot_human_counts = (
        window_df
        .groupby(["minute", "predicted_label"])
        .size()
        .unstack(fill_value=0)
        .sort_index()
    )

    st.line_chart(bot_human_counts)
else:
    st.warning("Brak eventów w wybranym oknie czasowym.")


st.subheader("Fałszywe alarmy w czasie")

if not window_df.empty:
    false_alarm_counts = (
        window_df
        .groupby("minute")["false_alarm"]
        .sum()
        .sort_index()
    )

    st.line_chart(false_alarm_counts)
else:
    st.warning("Brak danych o fałszywych alarmach.")


# -----------------------
# Recent events table
# -----------------------

st.subheader("Ostatnie predykcje")

columns_to_show = [
    "scored_at",
    "wiki",
    "title",
    "user",
    "actual_label",
    "predicted_label",
    "probability",
    "false_alarm"
]

available_columns = [
    col for col in columns_to_show
    if col in df.columns
]

recent_df = (
    df[available_columns]
    .sort_values("scored_at", ascending=False)
    .head(20)
)

st.dataframe(
    recent_df,
    use_container_width=True,
    hide_index=True
)


# -----------------------
# Raw data preview
# -----------------------

with st.expander("Podgląd surowych danych"):
    st.json(st.session_state["events"][-5:])


# -----------------------
# Auto-refresh
# -----------------------

if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()
