# generating fake data for telecom use cases
import random
import json
import yaml
import os
import csv
from faker import Faker
from datetime import datetime, timedelta
from kafka import KafkaProducer

fake = Faker()
MAROC_CITIES = ["CASABLANCA", "RABAT", "AGADIR", "MARRAKECH", "TANGER", "OUJDA"]
TECHNOLOGIES = ["2G", "3G", "4G", "5G"]
SERVICE_TYPES = ["voice", "sms", "data"]

def random_maroc_cell():
    city = random.choice(MAROC_CITIES)
    site = f"{random.randint(1, 50):02d}"
    return f"{city}_{site}"

def maroc_number():
    return "2126" + "".join([str(random.randint(0, 9)) for _ in range(8)])

def international_number():
    country_codes = ["33", "1", "44", "49", "34"]
    return "+" + random.choice(country_codes) + "".join([str(random.randint(0, 9)) for _ in range(9)])

def generate_voice_record():
    tech = random.choice(TECHNOLOGIES)
    return {
        "record_type": "voice",
        "timestamp": fake.date_time_between(start_date="-1h", end_date="now").isoformat() + "Z",
        "caller_id": maroc_number(),
        "callee_id": random.choice([maroc_number(), international_number()]),
        "duration_sec": random.randint(10, 60) if tech in ["2G", "3G"] else random.randint(30, 300),
        "cell_id": random_maroc_cell(),
        "technology": tech
    }

def generate_sms_record():
    return {
        "record_type": "sms",
        "timestamp": fake.date_time_between(start_date="-1h", end_date="now").isoformat() + "Z",
        "sender_id": maroc_number(),
        "receiver_id": random.choice([maroc_number(), international_number()]),
        "cell_id": random_maroc_cell(),
        "technology": random.choice(TECHNOLOGIES)
    }

def generate_data_record():
    tech = random.choice(TECHNOLOGIES)
    volume = {
        "2G": round(random.uniform(0.1, 5.0), 2),
        "3G": round(random.uniform(5.0, 100.0), 2),
        "4G": round(random.uniform(100.0, 800.0), 2),
        "5G": round(random.uniform(800.0, 2000.0), 2),
    }
    duration = {
        "2G": random.randint(30, 180),
        "3G": random.randint(100, 900),
        "4G": random.randint(300, 3600),
        "5G": random.randint(600, 7200),
    }
    return {
        "record_type": "data",
        "timestamp": fake.date_time_between(start_date="-1h", end_date="now").isoformat() + "Z",
        "user_id": maroc_number(),
        "data_volume_mb": volume[tech],
        "session_duration_sec": duration[tech],
        "cell_id": random_maroc_cell(),
        "technology": tech
    }

def inject_anomalies(record, anomaly_type):
    if not record or not isinstance(record, dict):
        return record

    if anomaly_type == "missing_fields":
        if len(record) > 1:
            record.pop(random.choice(list(record.keys())), None)

    elif anomaly_type == "corrupted_data":
        possible_fields = [f for f in ["duration_sec", "data_volume_mb", "session_duration_sec", "caller_id"] if f in record]
        if possible_fields:
            field = random.choice(possible_fields)
            record[field] = -1 if "duration" in field else "abcde"

    elif anomaly_type == "unknown_types":
        record["record_type"] = "ping"

    return record

def generate_records(config):
    output_path = config["data_generation"]["output_path"]
    os.makedirs(output_path, exist_ok=True)

    service_dist = config["service_distribution"]
    anomalies = config["anomaly_rates"]
    num_records = config["data_generation"]["num_records"]
    output_mode = config["data_generation"]["output_mode"]
    kafka_topic = config["data_generation"]["kafka_topic"]
    kafka_bootstrap = config["data_generation"]["kafka_bootstrap_servers"]

    records = []
    stats = {atype: 0 for atype in anomalies}

    for _ in range(num_records):
        rtype = random.choices(list(service_dist.keys()), weights=service_dist.values())[0]

        if rtype == "voice":
            record = generate_voice_record()
        elif rtype == "sms":
            record = generate_sms_record()
        else:
            record = generate_data_record()

        for anomaly_type, rate in anomalies.items():
            if anomaly_type == "duplicates":
                continue
            if random.random() < rate:
                record = inject_anomalies(record, anomaly_type)
                stats[anomaly_type] += 1

        records.append(record)

        if random.random() < anomalies.get("duplicates", 0):
            records.append(record.copy())
            stats["duplicates"] += 1

    if anomalies.get("out_of_order", 0) > 0:
        random.shuffle(records)
        stats["out_of_order"] = "shuffled"

    # Output
    if output_mode == "json":
        with open(os.path.join(output_path, "records.jsonl"), "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

    elif output_mode == "csv":
        keys = {k for r in records for k in r}
        with open(os.path.join(output_path, "records.csv"), "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(keys))
            writer.writeheader()
            writer.writerows(records)

    elif output_mode == "kafka":
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap,
                                 value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        for r in records:
            producer.send(kafka_topic, value=r)
        producer.flush()

    print(f"{len(records)} records generated to {output_mode}.")
    print("Anomaly statistics:", stats)

if __name__ == "__main__":
    with open("config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    generate_records(cfg)

