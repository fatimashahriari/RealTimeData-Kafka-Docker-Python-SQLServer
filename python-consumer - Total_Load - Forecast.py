import json
from kafka import KafkaConsumer
import pyodbc

# SQL Server connection
try:
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=KAFKA_ELECTRICITY;"
        "Trusted_Connection=yes;"
    )

    cursor_ = conn.cursor() # Test SQL server connection
    cursor_.execute("SELECT 1 AS test;")
    result = cursor_.fetchone()

    print("Connected! Result:", result)

except Exception as e:  
    print("Connection failed:", e)


# Check if table exists: xtype='U' --> (user-defined table)
cursor = conn.cursor()
cursor.execute("""
SELECT 1 
FROM sysobjects 
WHERE name='TOTAL_LOAD_FORECAST' AND xtype='U'
""")

if cursor.fetchone() is None:
    raise Exception("Table TOTAL_LOAD_FORECAST does not exist!")

#Get data from Kafka and insert into SQL Server
consumer = KafkaConsumer(
    "realtime-data-electricity-load_forecast",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

for data in consumer:
    msg = data.value
    rows = []

    for record in msg['data']:
        zone = record['zone']
        unit = record['unit']
        datetime = record['datetime']
        updatedAt = record['updatedAt']
        createdAt = record['createdAt']
        value = record['value']
        source = record['source']
        isEstimated = record['isEstimated']
        estimationMethod = record['estimationMethod']


        rows.append((zone, unit, datetime, updatedAt, createdAt, value, source, isEstimated, estimationMethod))

        cursor.executemany("""
            INSERT INTO KAFKA_ELECTRICITY.RAW_DATA.TOTAL_LOAD_FORECAST
            (zone, unit, datetime, updatedAt, createdAt, value, source, isEstimated, estimationMethod)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

        conn.commit()

        print(f"Inserted {len(rows)} rows for {updatedAt}")
