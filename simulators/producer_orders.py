import json
import time
import random
import uuid  # لإنشاء IDs فريدة تماماً
from kafka import KafkaProducer

# 1. إعداد المنتج للربط مع كافكا
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all'
)

TOPIC_NAME = 'order_metadata'

def generate_unique_orders():
    # قائمة المناديب اللي بنحاكي حركتهم
    couriers = ['C-101', 'C-102', 'C-103']
    
    print(f"\n📦 [NEW BATCH] Generating unique orders for couriers...")
    
    for cid in couriers:
        # إنشاء Order ID فريد (مثال: ORD-a1b2c3...)
        unique_id = f"ORD-{str(uuid.uuid4())[:8].upper()}"
        
        # اختيار قيمة عشوائية (عشان نجرب الـ Filter في السبارك)
        # السرعة > 100 والقيمة > 2000 هي اللي هتعمل Alert
        random_value = random.choice([800.0, 1500.0, 3200.0, 5500.0, 7000.0])
        
        order_data = {
            "courier_id": cid,
            "order_id": unique_id,
            "value": float(random_value)
        }
        
        # إرسال البيانات لكافكا
        producer.send(TOPIC_NAME, value=order_data)
        
        print(f"✅ Sent -> Courier: {cid} | Order: {unique_id} | Value: {random_value} EGP")
    
    # التأكد من وصول البيانات فوراً
    producer.flush()

if __name__ == "__main__":
    try:
        print("🚀 Orders Simulator is running... Press Ctrl+C to stop.")
        while True:
            generate_unique_orders()
            
            # بنغير الأوردرات كل 30 ثانية عشان نحاكي الواقع
            # المندوب بياخد وقت عقبال ما يخلص أوردر ويستلم التاني
            print("⏳ Waiting 30 seconds for next order cycle...")
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping Orders Producer...")
    finally:
        producer.close()