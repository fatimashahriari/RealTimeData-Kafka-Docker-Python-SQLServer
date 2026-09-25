from modules.class_ElectricityProducer import ElectricityProducer

URLS = {
    "flows": "https://api.electricitymaps.com/v4/electricity-flows/latest",
    "load": "https://api.electricitymaps.com/v4/total-load/latest",
    "flows_history": "https://api.electricitymaps.com/v4/electricity-flows/past-range?zone=DK-DK2&start=2024-05-19T03%3A00%3A00Z&end=2026-09-23T08%3A00%3A00Z",
    "load_history": "https://api.electricitymaps.com/v4/total-load/past-range?zone=DK-DK2&start=2024-05-19T03%3A00%3A00Z&end=2026-09-23T08%3A00%3A00Z",
    "flows_forecast": "https://api.electricitymaps.com/v4/electricity-flows/forecast",
    "load_forecast": "https://api.electricitymaps.com/v4/total-load/forecast"
}

if __name__ == "__main__":
    producer = ElectricityProducer(URLS)
    producer.run()
