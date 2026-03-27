import json
import time
import random
from kafka import KafkaProducer

# إعداد المنتج للربط مع كافكا (Local Testing)
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'courier_telemetry'

def run_telemetry():
    couriers = ['C-101', 'C-102', 'C-103']
    print(f"🚀 Telemetry Simulator Active...")
    while True:
        for cid in couriers:
            data = {
                "courier_id": cid,
                "speed": random.randint(0, 120),
                "lat": round(random.uniform(30.0, 30.1), 6),
                "long": round(random.uniform(31.2, 31.3), 6),
                "timestamp": time.time()
            }
            producer.send(TOPIC_NAME, value=data)
            print(f" Sent data for {cid}: speed={data['speed']}")
        producer.flush()
        time.sleep(1) # سرعة عالية للتجربة

if __name__ == "__main__":
    run_telemetry()