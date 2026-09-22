Here is your **downloadable `README.md` file**, Fatemeh — exactly as a GitHub‑ready Markdown document.  
You can copy it into a file named **README.md** and save it directly.

---

```markdown
# 📘 Real‑Time Kafka → SQL Server Pipeline (Electricity Flows)

This project demonstrates a complete real‑time data pipeline:

**ElectricityMaps API → Kafka (Docker) → Python Consumer → SQL Server**

It includes:

- `docker-compose.yml` — Kafka, Zookeeper, Kafka‑UI  
- `python-producer.py` — Streams live electricity flow data into Kafka  
- `python-consumer.py` — Reads Kafka messages and inserts them into SQL Server  
- SQL scripts — Create database, schema, and table  

---

## 🚀 1. Prerequisites

Install the following:

### ✔ Docker Desktop  
Required to run Kafka, Zookeeper, and Kafka‑UI.

### ✔ Python 3.10+  
Install dependencies:

```bash
pip install kafka-python pyodbc requests
```

### ✔ SQL Server  
You can use:

- SQL Server Developer Edition (Windows)
- SQL Server in Docker

### ✔ ElectricityMaps API Key  
Set your API key:

```bash
setx API_KEY "your_api_key_here"
```

---

## 🗂 2. Project Structure

```
realtime-kafka-docker-sqlserver-pipeline/
│
├── docker-compose.yml
├── python-producer.py
├── python-consumer.py
└── README.md
```

---

## 🐳 3. Start Kafka + Zookeeper + Kafka UI

Make sure Docker Desktop is running.

Start the stack:

```bash
docker compose up -d
```

This launches:

| Service | Port | Description |
|--------|------|-------------|
| Zookeeper | 2181 | Kafka coordination |
| Kafka | 9092 | Message broker |
| Kafka UI | 8080 | Web interface |

Open Kafka UI:

```
http://localhost:8080
```

---

## ⚙️ 4. Kafka Configuration (docker-compose.yml)

Kafka is configured to be reachable from Windows Python scripts:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
```

This is required because Kafka runs in Docker (Linux) and Python runs on Windows.

---

## 🗄 5. SQL Server Setup

Run the following SQL script in SSMS:

```sql
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'KAFKA_ELECTRICITY')
BEGIN
    CREATE DATABASE KAFKA_ELECTRICITY;
END
GO

USE KAFKA_ELECTRICITY;
GO

IF NOT EXISTS (SELECT name FROM sys.schemas WHERE name = 'RAW_DATA')
BEGIN
    CREATE SCHEMA RAW_DATA;
END
GO

IF NOT EXISTS(
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'ELECTRICITY_FLOWS' AND s.name = 'RAW_DATA'
)
BEGIN
    CREATE TABLE RAW_DATA.ELECTRICITY_FLOWS(
        zone NVARCHAR(50),
        temporalGranularity NVARCHAR(10),
        unit NVARCHAR(10),
        datetime DATETIME2,
        updatedAt DATETIME2,
        value FLOAT,
        import BIT,
        export BIT
    );
END
GO
```

---

## 📡 6. Python Producer — Stream Data Into Kafka

The producer fetches live electricity flow data from ElectricityMaps API and sends it to Kafka every 5 minutes.

Run the producer:

```bash
python python-producer.py
```

You should see:

```
Sent: 2026-09-22T18:00:00Z
```

---

## 🧩 7. Python Consumer — Insert Kafka Messages Into SQL Server

The consumer listens to Kafka and inserts each message into SQL Server.

Run it:

```bash
python python-consumer.py
```

You should see:

```
Connected! Result: (1,)
Inserted 24 rows for 2026-09-22T18:00:00Z
```

---

## 🔍 8. Verify Data in SQL Server

Run:

```sql
SELECT TOP 50 * FROM KAFKA_ELECTRICITY.RAW_DATA.ELECTRICITY_FLOWS;
```

You should see rows for:

- zone  
- temporalGranularity  
- unit  
- datetime  
- updatedAt  
- value  
- import/export flags  

---

## 📊 9. View Kafka Messages (Optional)

To inspect messages:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic realtime-data-electricity-flows \
  --from-beginning
```

---

## 🧱 10. Troubleshooting

### ❌ Python producer cannot reach Kafka  
Error:

```
KafkaTimeoutError: Failed to update metadata
```

Fix: Docker Desktop must be running.

### ❌ Consumer error: “Invalid object name”  
Use:

```
RAW_DATA.ELECTRICITY_FLOWS
```

not:

```
ELECTRICITY_FLOWS
```

### ❌ Consumer error: “tuple indices must be integers”  
Use:

```python
msg = data.value
```

not:

```python
data['zone']
```

---

## 🎉 11. Summary

This project demonstrates a full real‑time streaming pipeline:

```
ElectricityMaps API → Kafka → Python Consumer → SQL Server
```

You now have:

- Real‑time ingestion  
- Real‑time storage  
- Dockerized Kafka cluster  
- SQL Server integration  
- Kafka UI visualization  

---
```

---

If you want, I can also generate:

- a downloadable ZIP containing all files  
- a GitHub description section  
- a project architecture diagram  
- a “How it works” animation  
- badges for your GitHub repo  

Just tell me what you want next, Fatemeh.