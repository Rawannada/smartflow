import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'courier_telemetry'

def run_egyptian_telemetry():
    couriers = [
        'Ahmed_Zaki', 'Mohamed_Ramadan', 'Mahmoud_El_Sisi', 
        'Mostafa_Bakry', 'Tarek_Hamed', 'Hassan_Shaker', 
        'Ibrahim_Adel', 'Ali_Mazhar', 'Sayed_Abdelhafiz', 'Amr_Diab'
    ]
    
    print(f"🚀 Egypt Telemetry Simulator Active (10 Couriers)...")
    
    while True:
        for cid in couriers:
            if cid in ['Ahmed_Zaki', 'Ali_Mazhar']:
                speed = random.randint(40, 70)
            else:
                speed = random.randint(80, 145)

            data = {
                "courier_id": cid,
                "speed": speed,
                "lat": round(random.uniform(30.0, 30.1), 6),
                "long": round(random.uniform(31.2, 31.3), 6),
                "timestamp": time.time()
            }
            
            producer.send(TOPIC_NAME, value=data)
            print(f"📡 Telemetry -> {cid}: Speed={speed} km/h")
            
        producer.flush()
        time.sleep(2) 

if __name__ == "__main__":
    try:
        run_egyptian_telemetry()
    except KeyboardInterrupt:
        print("\n🛑 Telemetry Stopped.")
    finally:
        producer.close()