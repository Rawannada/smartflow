import json
import time
import random
import uuid
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

TOPIC_NAME = 'order_metadata'

def generate_egyptian_logistics_data():
    couriers = [
        'Ahmed_Zaki', 'Mohamed_Ramadan', 'Mahmoud_El_Sisi', 
        'Mostafa_Bakry', 'Tarek_Hamed', 'Hassan_Shaker', 
        'Ibrahim_Adel', 'Ali_Mazhar', 'Sayed_Abdelhafiz', 'Amr_Diab'
    ]
    
    print(f"\n📦 [NEW BATCH] Simulating real-time orders for Egypt Branch...")
    
    for courier in couriers:
        unique_id = f"ORD-{str(uuid.uuid4())[:8].upper()}"
        
        random_value = random.choice([
            120.0, 350.0, 600.0,      
            1500.0, 2800.0, 4200.0,   
            6500.0, 9000.0, 14500.0   
        ])
        
        order_data = {
            "courier_id": courier,
            "order_id": unique_id,
            "value": float(random_value),
            "timestamp": time.time()
        }
        
        producer.send(TOPIC_NAME, value=order_data)
        print(f"✅ Sent -> Courier: {courier} | Order: {unique_id} | Value: {random_value} EGP")
    
    producer.flush()

if __name__ == "__main__":
    try:
        print("🚀 Egypt Logistics Simulator Running... Press Ctrl+C to stop.")
        while True:
            generate_egyptian_logistics_data()
            print("⏳ Next delivery cycle in 10 seconds...")
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping Egypt Producer...")
    finally:
        producer.close()