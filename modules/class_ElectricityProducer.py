import json
import time
import requests
from kafka import KafkaProducer
import os
from concurrent.futures import ThreadPoolExecutor

class ElectricityProducer:
    def __init__(self, urls):
        self.urls = urls
        self.api_key = os.environ["API_KEY"]
        self.headers = {"auth-token": self.api_key}

        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def fetch(self, url):
        response = requests.get(url, headers=self.headers)
        return response.json()

    def send(self, topic, data):
        self.producer.send(topic, data)

    def run(self):
        while True:
            with ThreadPoolExecutor() as executor:
                futures = {
                    name: executor.submit(self.fetch, url)
                    for name, url in self.urls.items()
                }

                results = {name: future.result() for name, future in futures.items()}

            for name, data in results.items():
                topic = f"realtime-data-electricity-{name}"
                self.send(topic, data)
                from datetime import datetime
                print(f"Sent {name.upper()} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            time.sleep(300)
