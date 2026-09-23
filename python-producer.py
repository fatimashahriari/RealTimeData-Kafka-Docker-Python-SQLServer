from modules.class_ElectricityProducer import ElectricityProducer

URLS = {
    "flows": "https://api.electricitymaps.com/v4/electricity-flows/latest",
    "load": "https://api.electricitymaps.com/v4/total-load/latest"
}

if __name__ == "__main__":
    producer = ElectricityProducer(URLS)
    producer.run()
