import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/factory")

TOPIC_STATION = "station-events"
TOPIC_SUPPLIER = "supplier-events"
TOPIC_TEAM = "team-status"

# Ordered line: a unit must visit these in order.
STATIONS = [
    {"station_id": "ST-01", "event_type": "ASSEMBLY",  "team": "Assembly"},
    {"station_id": "ST-02", "event_type": "QC",        "team": "QC"},
    {"station_id": "ST-03", "event_type": "PACKAGING", "team": "Packaging"},
]
STATION_INDEX = {s["station_id"]: i for i, s in enumerate(STATIONS)}
TEAMS = ["Assembly", "QC", "Packaging", "Logistics"]

MAX_GAP_SECONDS = float(os.getenv("MAX_GAP_SECONDS", "45"))      # max time between consecutive stations
SLA_THRESHOLD_MIN = float(os.getenv("SLA_THRESHOLD_MIN", "15"))  # lateness allowed on deliveries
SLIP_AMBER_MIN = float(os.getenv("SLIP_AMBER_MIN", "30"))
SLIP_RED_MIN = float(os.getenv("SLIP_RED_MIN", "120"))
RED_BLOCKERS = int(os.getenv("RED_BLOCKERS", "2"))
PBI_PUSH_INTERVAL = float(os.getenv("PBI_PUSH_INTERVAL", "3"))   # seconds between pushes per stream
