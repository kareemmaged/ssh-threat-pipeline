import json
import re
from datetime import datetime
import psycopg2
from kafka import KafkaConsumer

LINE_RE = re.compile(
    r"^(?P<ts>\w+\s+\d+\s+[\d:]+)\s+(?P<host>\S+)\s+sshd\[(?P<pid>\d+)\]:\s(?P<msg>.*)$"
)
FAILED_PW = re.compile(
    r"Failed password for (?:invalid user )?(?P<user>\S+) from (?P<ip>\S+) port (?P<port>\d+)"
)
INVALID_USER = re.compile(r"Invalid user (?P<user>\S+) from (?P<ip>\S+)")
YEAR = datetime.now().year  # log lines have no year, use current one


def parse_line(raw):
    m = LINE_RE.match(raw)
    if not m:
        return None
    d = {
        "event_ts": datetime.strptime(f"{YEAR} {m['ts']}", "%Y %b %d %H:%M:%S"),
        "hostname": m["host"],
        "pid": int(m["pid"]),
        "username": None,
        "source_ip": None,
        "source_port": None,
        "raw_message": m["msg"],
    }
    msg = m["msg"]
    if FAILED_PW.search(msg):
        f = FAILED_PW.search(msg)
        d.update(
            event_type="failed_password",
            username=f["user"],
            source_ip=f["ip"],
            source_port=int(f["port"]),
        )
    elif INVALID_USER.search(msg):
        f = INVALID_USER.search(msg)
        d.update(event_type="invalid_user", username=f["user"], source_ip=f["ip"])
    elif "POSSIBLE BREAK-IN ATTEMPT" in msg:
        d["event_type"] = "reverse_dns_fail"
    elif "authentication failure" in msg:
        d["event_type"] = "pam_auth_failure"
    elif "Connection closed" in msg or "disconnect" in msg.lower():
        d["event_type"] = "connection_closed"
    else:
        d["event_type"] = "other"
    return d


conn = psycopg2.connect(
    host="localhost", dbname="secops", user="pipeline", password="pipeline"
)
cur = conn.cursor()

consumer = KafkaConsumer(
    "ssh-logs",
    group_id="parser-group",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

print("Consuming from Kafka → Postgres ...")
for msg in consumer:
    parsed = parse_line(msg.value["raw_line"])
    if parsed:
        cur.execute(
            """INSERT INTO raw_ssh_events
               (event_ts, hostname, pid, event_type, username, source_ip, source_port, raw_message)
               VALUES (%(event_ts)s, %(hostname)s, %(pid)s, %(event_type)s,
                       %(username)s, %(source_ip)s, %(source_port)s, %(raw_message)s)""",
            parsed,
        )
        conn.commit()
        print(
            f"stored [{parsed['event_type']}] {parsed['username']} from {parsed['source_ip']}"
        )
