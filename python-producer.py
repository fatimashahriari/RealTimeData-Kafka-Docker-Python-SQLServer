import json
import time
import requests
from kafka import KafkaProducer
import os



API_KEY = os.environ["API_KEY"]


url = "https://api.electricitymaps.com/v4/electricity-flows/latest"
#headers = {"auth-token" : "em_Md4SURK64FGXUWCdWbdMjKGUS7FYdBXX"}
headers = {"auth-token" : API_KEY}


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_data():
    response = requests.request("GET", url, headers= headers)
    return response.json()

while True: 
    data = get_data()
    producer.send("realtime-data-electricity-flows", data)
    print("Sent:", data["data"][0]["datetime"])
    time.sleep(300)