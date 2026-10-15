import json
import httpx
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Consuming API stream → producing to Kafka topic 'ssh-logs' ...")
with httpx.stream("GET", "http://localhost:8000/logs/stream") as response:
    for line in response.iter_lines():
        if line.startswith("data: "):
            raw_log_line = line[len("data: "):]
            producer.send("ssh-logs", value={"raw_line": raw_log_line})
            print(f"sent: {raw_log_line[:80]}")

producer.flush()
print("Stream finished.")