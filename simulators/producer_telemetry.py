import json
import time
import random
from kafka import KafkaProducer

# 1. إعداد المنتج (Telemetry)
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'courier_telemetry'

def run_egyptian_telemetry():
    # نفس الأسماء اللي في الـ Orders Producer بالظبط
    couriers = [
        'Ahmed_Zaki', 'Mohamed_Ramadan', 'Mahmoud_El_Sisi', 
        'Mostafa_Bakry', 'Tarek_Hamed', 'Hassan_Shaker', 
        'Ibrahim_Adel', 'Ali_Mazhar', 'Sayed_Abdelhafiz', 'Amr_Diab'
    ]
    
    print(f"🚀 Egypt Telemetry Simulator Active (10 Couriers)...")
    
    while True:
        for cid in couriers:
            # تنويع السرعات لزوم الـ "Risk Analysis"
            # فيه الهادي (40-60) وفيه الطاير (100-140)
            if cid in ['Ahmed_Zaki', 'Ali_Mazhar']: # الناس الهادية
                speed = random.randint(40, 70)
            else: # الباقي متهور شوية للمناقشة
                speed = random.randint(80, 145)

            data = {
                "courier_id": cid,
                "speed": speed,
                "lat": round(random.uniform(30.0, 30.1), 6), # إحداثيات في القاهرة
                "long": round(random.uniform(31.2, 31.3), 6),
                "timestamp": time.time()
            }
            
            producer.send(TOPIC_NAME, value=data)
            print(f"📡 Telemetry -> {cid}: Speed={speed} km/h")
            
        producer.flush()
        # خليه يبعت كل ثانيتين عشان ميزحمش الـ Spark أوي
        time.sleep(2) 

if __name__ == "__main__":
    try:
        run_egyptian_telemetry()
    except KeyboardInterrupt:
        print("\n🛑 Telemetry Stopped.")
    finally:
        producer.close()