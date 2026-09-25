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
WHERE name='ELECTRICITY_FLOWS_FORECAST' AND xtype='U'
""")

if cursor.fetchone() is None:
    raise Exception("Table ELECTRICITY_FLOWS_FORECAST does not exist!")


#Get data from Kafka and insert into SQL Server
consumer = KafkaConsumer(
    "realtime-data-electricity-flows_forecast",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

for event in consumer:
    msg = event.value
    rows = []

    zone = msg['zone']
    temporalGranularity = msg['temporalGranularity']
    unit = msg['unit']
    for record in msg['data']:

        datetime = record['datetime']
        updatedAt = record['updatedAt']

        for key, value in record['import'].items():
            rows.append((
                zone,
                temporalGranularity,
                unit,
                datetime,
                updatedAt,
                value,
                key,
                ""
            ))

        for key, value in record['export'].items():
            rows.append((
                zone,
                temporalGranularity,
                unit,
                datetime,
                updatedAt,
                value,
                "",
                key
            ))

        cursor.executemany("""
            INSERT INTO KAFKA_ELECTRICITY.RAW_DATA.ELECTRICITY_FLOWS_FORECAST
            (zone, temporalGranularity, unit, datetime, updatedAt, value, import, export)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)

        conn.commit()

        print(f"Inserted {len(rows)} rows for {updatedAt}")
