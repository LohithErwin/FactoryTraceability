# Real-Time Factory Traceability & Coordination Platform

A real-time event-driven platform for **factory traceability, production integrity monitoring, supplier SLA monitoring, and team health tracking**.

The system simulates a smart manufacturing environment where production units, supplier deliveries, and team activities continuously generate events. These events are streamed through **Apache Kafka**, processed by Python-based monitoring services, stored in **PostgreSQL**, and visualized through **Microsoft Power BI**.

---

## 🚀 Overview

Modern manufacturing systems generate large volumes of operational events across production stations, suppliers, and engineering teams. Detecting issues such as incorrect production sequences, duplicate processing, supplier delays, blocked deliveries, and team bottlenecks requires continuous monitoring.

This project implements a real-time monitoring pipeline that:

* Simulates factory production events
* Tracks units across multiple production stations
* Detects production traceability violations
* Monitors supplier delivery SLAs
* Detects blocked and late supplier deliveries
* Tracks team blockers and ETA slippage
* Calculates team health using `GREEN / AMBER / RED` status
* Stores raw events and derived incidents in PostgreSQL
* Streams live metrics to Power BI
* Provides deterministic anomaly injection for demonstrations and testing

---

## 🏗️ System Architecture

```text
                         ┌──────────────────────┐
                         │   Factory Simulator   │
                         │  Station Producer    │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │                      │
                         │       Apache         │
                         │        Kafka         │
                         │                      │
                         └──────────┬───────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
              ▼                     ▼                     ▼
       station-events        supplier-events          team-status
              │                     │                     │
              ▼                     ▼                     ▼
     Integrity Checker        SLA Monitor          Team Aggregator
              │                     │                     │
              ▼                     ▼                     ▼
       ┌──────────────────────────────────────────────────────┐
       │                     PostgreSQL                       │
       │                                                      │
       │ Raw Events + Violations + SLA Breaches + Team State  │
       └──────────────────────────┬───────────────────────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │    Power BI      │
                         │ Live Monitoring  │
                         │ & Historical     │
                         │    Analytics     │
                         └──────────────────┘
```

---

## 🔄 Event Processing Pipeline

The platform is divided into three independent monitoring pipelines.

### 1. Factory Traceability

```text
Station Producer
      ↓
station-events
      ↓
Integrity Checker
      ↓
PostgreSQL
      ↓
Power BI
```

The integrity checker monitors production units moving through:

```text
ST-01 → ST-02 → ST-03
```

It detects:

* Duplicate processing
* Skipped stations
* Out-of-sequence processing
* Excessive time gaps
* Unknown stations

It also tracks production throughput and QC failures.

---

### 2. Supplier SLA Monitoring

```text
Supplier Producer
      ↓
supplier-events
      ↓
SLA Monitor
      ↓
PostgreSQL
      ↓
Power BI
```

The supplier simulator generates:

* On-time deliveries
* Late deliveries
* Overdue deliveries
* Blocked deliveries
* Completed deliveries

The SLA Monitor detects and tracks:

* `LATE` breaches
* `BLOCKED` breaches
* Open breaches
* Resolved breaches
* Delivery delays

### What is an SLA?

**SLA (Service Level Agreement)** represents the expected service/delivery level.

For example:

```text
Expected delivery: 10:00
Actual/current time: 10:30

Delay = 30 minutes
```

If the configured SLA threshold is 15 minutes:

```text
30 > 15
   ↓
SLA Breach
```

---

### 3. Team Health Monitoring

```text
Team Status Producer
      ↓
team-status
      ↓
Status Aggregator
      ↓
PostgreSQL
      ↓
Power BI
```

The team simulator generates:

* Normal progress
* ETA slippage
* Blockers
* Blocker resolution
* At-risk tasks

The Team Status Aggregator calculates:

* Number of open blockers
* Average ETA slippage
* Maximum ETA slippage
* Overall team health

Team health is represented as:

```text
GREEN  → Healthy
AMBER  → At risk
RED    → Serious operational issue
```

The exact thresholds are configurable in `config.py`.

---

# 📁 Project Structure

```text
factory-traceability/
│
├── common/
│   ├── kafka_utils.py
│   ├── pg.py
│   └── powerbi.py
│
├── consumers/
│   ├── integrity_checker.py
│   ├── sla_monitor.py
│   └── status_aggregator.py
│
├── producers/
│   ├── station_producer.py
│   ├── supplier_producer.py
│   ├── team_status_producer.py
│   └── demo_inject.py
│
├── db/
│   ├── init_db.py
│   └── migrations/
│       ├── 001_init.sql
│       └── 002_views.sql
│
├── powerbi/
│   └── measures.dax
│
├── scripts/
│   ├── create_topics.py
│   ├── create_pbi_datasets.py
│   └── run_all.py
│
├── config.py
├── requirements.txt
├── .env.example
└── README.md
```

---

# 🧩 Component Description

## `common/`

Contains shared infrastructure used by producers and consumers.

### `kafka_utils.py`

Provides reusable Kafka functionality:

* Kafka producer creation
* Kafka message publishing
* Kafka consumer loop
* JSON serialization/deserialization
* Graceful shutdown
* Periodic callbacks

---

### `pg.py`

Provides PostgreSQL database utilities used by consumers.

It handles:

* Database connections
* SQL execution
* Query execution

---

### `powerbi.py`

Provides the Power BI push interface.

It supports:

* Direct Power BI push URLs
* Power BI REST API datasets
* Authentication tokens
* Token caching
* Push throttling
* Failure handling

Power BI failures do not terminate the Kafka consumers.

---

# 📡 Producers

## `station_producer.py`

Simulates units moving through the production line:

```text
ST-01 → ST-02 → ST-03
```

It generates:

* Normal station events
* QC pass/fail events
* Duplicate events
* Skipped stations
* Excessive time gaps

Example:

```bash
python -m producers.station_producer
```

Custom configuration:

```bash
python -m producers.station_producer \
    --rate 5 \
    --anomaly-rate 0.20 \
    --max-events 500
```

Arguments:

| Argument         | Description                         |
| ---------------- | ----------------------------------- |
| `--rate`         | Events generated per second         |
| `--anomaly-rate` | Probability of anomaly injection    |
| `--max-events`   | Maximum events; `0` means unlimited |

---

## `supplier_producer.py`

Simulates supplier deliveries.

It generates:

* On-time deliveries
* Late deliveries
* Blocked deliveries
* Eventually delivered shipments

Run:

```bash
python -m producers.supplier_producer
```

Change the delivery interval:

```bash
python -m producers.supplier_producer --interval 3
```

---

## `team_status_producer.py`

Simulates operational team status.

It generates:

* Normal progress
* ETA slippage
* Blockers
* Blocker resolution
* At-risk tasks

Run:

```bash
python -m producers.team_status_producer
```

Change event rate:

```bash
python -m producers.team_status_producer --rate 2
```

---

# 🧪 Demo Anomaly Injection

## `demo_inject.py`

Provides deterministic anomalies for demonstrations and testing.

Supported scenarios:

```text
dup
skip
gap
late
blocked
redteam
all
```

### Duplicate station

```bash
python -m producers.demo_inject dup
```

Simulates:

```text
ST-01 → ST-01
```

Expected detection:

```text
DUPLICATE_SERIAL
```

---

### Skipped station

```bash
python -m producers.demo_inject skip
```

Simulates:

```text
ST-01 → ST-03
```

Expected detection:

```text
OUT_OF_SEQUENCE
```

---

### Excessive time gap

```bash
python -m producers.demo_inject gap
```

Simulates an unusually long delay between stations.

Expected detection:

```text
TIME_GAP
```

---

### Late supplier

```bash
python -m producers.demo_inject late
```

Creates an overdue supplier delivery.

Expected detection:

```text
LATE
```

---

### Blocked supplier

```bash
python -m producers.demo_inject blocked
```

Creates a blocked supplier delivery.

Expected detection:

```text
BLOCKED
```

---

### RED team

```bash
python -m producers.demo_inject redteam
```

Creates multiple blockers and significant ETA slippage for the Packaging team.

Expected result:

```text
Packaging → RED
```

---

### Run all scenarios

```bash
python -m producers.demo_inject all
```

---

# 🔍 Consumers

## `integrity_checker.py`

Consumes:

```text
station-events
```

Checks:

* Unknown stations
* Duplicate serial numbers
* Out-of-sequence movement
* Negative time gaps
* Excessive station-to-station gaps

Stores:

```text
station_events
violations
```

It also generates live factory metrics for Power BI.

---

## `sla_monitor.py`

Consumes:

```text
supplier-events
```

Monitors:

* Supplier delivery timing
* Late deliveries
* Blocked deliveries
* Open SLA breaches
* Resolved breaches

Stores:

```text
supplier_events
sla_breaches
```

---

## `status_aggregator.py`

Consumes:

```text
team-status
```

Calculates:

```text
Open blockers
Average ETA slippage
Maximum ETA slippage
Team health
```

Stores:

```text
team_status_events
team_status_rollup
team_status_history
```

---

# 🗄️ PostgreSQL Data Model

The database stores both **raw events** and **derived operational intelligence**.

### Raw event tables

```text
station_events
supplier_events
team_status_events
```

These answer:

> "What actually happened?"

### Derived tables

```text
violations
sla_breaches
team_status_rollup
team_status_history
```

These answer:

> "What problems did the system detect and what is the current state?"

---

# 📊 PostgreSQL Views

The project also creates analytical views:

```text
v_throughput_minute
v_violations_trend
v_sla_breaches
v_team_health_trend
```

These are used for historical Power BI reporting.

---

# 📈 Power BI

The project supports both **live monitoring** and **historical analysis**.

### Live datasets

Three Power BI streaming datasets are created:

```text
Factory Live - Metrics
Factory Live - SLA
Factory Live - Teams
```

Create them using:

```bash
python -m scripts.create_pbi_datasets
```

The script prints:

```text
PBI_DATASET_METRICS=...
PBI_DATASET_SLA=...
PBI_DATASET_TEAMS=...
```

These values can be added to `.env`.

---

## Historical Power BI Measures

Example measures include:

```DAX
Total Events =
SUM(v_throughput_minute[events])

QC Fails =
SUM(v_throughput_minute[fails])

Total Violations =
SUM(v_violations_trend[violations])

Violations per 1k =
DIVIDE([Total Violations], [Total Events]) * 1000

Open SLA Breaches =
CALCULATE(
    COUNTROWS(v_sla_breaches),
    v_sla_breaches[status] = "OPEN"
)

Avg SLA Delay (min) =
AVERAGE(v_sla_breaches[delay_minutes])

Worst Team Health =
MAX(v_team_health_trend[health_score])

Avg ETA Slip (min) =
AVERAGE(v_team_health_trend[avg_eta_slippage_min])
```

---

# 📊 Recommended Dashboard Visuals

### Factory Monitoring

* Events per minute
* QC failures
* Total violations
* Violations per 1,000 events
* Violations by type
* Station throughput

### Supplier Monitoring

* Total deliveries
* Open SLA breaches
* Late deliveries
* Blocked deliveries
* Average SLA delay
* Breaches by type/status

### Team Monitoring

* Team health
* Open blockers
* Average ETA slippage
* Maximum ETA slippage
* Team health trend
* Team × time health matrix

---

# ⚙️ Configuration

Project configuration is centralized in:

```text
config.py
```

Important configuration includes:

```text
Kafka bootstrap server
PostgreSQL DATABASE_URL
Kafka topic names
Factory stations
Team names
Station sequence
Maximum station gap
SLA threshold
Team health thresholds
Power BI push interval
```

---

# 🔐 Environment Variables

Create a `.env` file based on `.env.example`.

Example:

```env
DATABASE_URL=postgresql://postgres:<your_password>@localhost:5432/factory

KAFKA_BOOTSTRAP=localhost:9092

PBI_ACCESS_TOKEN=your_token

PBI_WORKSPACE_ID=your_workspace_id

PBI_DATASET_METRICS=your_metrics_dataset_id
PBI_DATASET_SLA=your_sla_dataset_id
PBI_DATASET_TEAMS=your_teams_dataset_id
```



---


---

# 🎯 Key Design Principles

### Event-driven architecture

Kafka decouples event producers from monitoring services.

### Separation of concerns

Each consumer has a specific responsibility:

```text
Integrity → Factory traceability
SLA       → Supplier performance
Teams     → Operational team health
```

### Raw + derived data

The system preserves raw events while separately storing detected problems and aggregated states.

### Fault isolation

Power BI failures do not stop Kafka event processing.

### Configurable simulation

Event rate, anomaly rate, thresholds, and other operational parameters can be configured.

### Reproducible demonstrations

`demo_inject.py` provides deterministic anomaly scenarios for testing and presentations.

---

# 🧠 Example Scenario

Consider a production unit:

```text
SN001
```

The normal flow is:

```text
ST-01 → ST-02 → ST-03
```

Suppose the producer generates:

```text
ST-01
ST-03
```

The event reaches Kafka:

```text
station-events
```

The Integrity Checker detects:

```text
OUT_OF_SEQUENCE
```

and stores:

```text
violations
```

Power BI then reflects the increased violation count.

At the same time, suppose a supplier delivery is 45 minutes late.

The SLA Monitor detects:

```text
LATE
```

and creates an:

```text
OPEN SLA BREACH
```

Meanwhile, a team may have:

```text
2 open blockers
+
60 minute maximum ETA slip
```

The Team Status Aggregator calculates:

```text
RED
```

The final dashboard therefore provides a consolidated view of:

```text
Factory integrity
       +
Supplier performance
       +
Team operational health
```

---

# 🔬 Project Focus

This project demonstrates the integration of:

* **Event-driven architecture**
* **Apache Kafka**
* **Python stream processing**
* **PostgreSQL**
* **Real-time operational monitoring**
* **Data quality/integrity validation**
* **Supplier SLA monitoring**
* **Team health aggregation**
* **Power BI**
* **Synthetic event simulation**
* **Anomaly injection**

It can be extended toward industrial applications such as manufacturing traceability, production monitoring, supplier coordination, factory digital twins, and operational intelligence.

