from datetime import datetime


def create_message(value):
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "source": "SIMULADOR",
        "variable": "SST",
        "value": value,
        "unit": "°C",
        "location": {
            "lat": -2.170998,
            "lon": -79.922359
        }
    }