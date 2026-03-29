import smtplib
from email.message import EmailMessage
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, current_timestamp, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def send_alert_email(courier_id, speed, order_id):
    msg = EmailMessage()
    msg.set_content(f"🚨 ALERT: Courier {courier_id} is Overspeeding!\n\n"
                    f"Details:\n"
                    f"- Speed: {speed} km/h\n"
                    f"- Order ID: {order_id}\n"
                    f"- Risk Status: HIGH RISK\n\n"
                    f"Please take immediate action to ensure safety.")
    
    msg['Subject'] = f"⚠️ SmartFlow Alert: High Risk Courier {courier_id}"
    msg['From'] = "rwannada22@gmail.com"  
    msg['To'] = "rwannada2222@gmail.com"   
    
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login("rwannada22@gmail.com", "tcuacqogbwcsxivk") 
            smtp.send_message(msg)
            print(f"📧 Email Alert Sent for Courier: {courier_id}")
    except Exception as e:
        print(f"❌ Email Failed for {courier_id}: {e}")

spark = SparkSession.builder \
    .appName("SmartFlow-Final-Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

telemetry_schema = StructType([
    StructField("courier_id", StringType()),
    StructField("speed", DoubleType()),
    StructField("timestamp", DoubleType())
])

order_schema = StructType([
    StructField("courier_id", StringType()),
    StructField("order_id", StringType()),
    StructField("value", DoubleType())
])

telemetry_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "courier_telemetry") \
    .load() \
    .select(from_json(col("value").cast("string"), telemetry_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp")) \
    .withWatermark("event_time", "5 minutes")

orders_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "order_metadata") \
    .load() \
    .select(from_json(col("value").cast("string"), order_schema).alias("data")) \
    .select(col("data.courier_id").alias("o_id"), "data.order_id", "data.value") \
    .withColumn("order_time", current_timestamp()) \
    .withWatermark("order_time", "5 minutes")

final_alerts = telemetry_df.join(
    orders_df,
    expr("""
        courier_id = o_id AND
        event_time >= order_time - interval 10 minutes AND
        event_time <= order_time + interval 10 minutes
    """)
).filter((col("speed") > 100) & (col("value") > 2000)) \
 .dropDuplicates(["courier_id", "order_id"])

def process_batch(df, epoch_id):
    if df.count() > 0:
        print(f"⚡ Processing {df.count()} new alerts...")

        df_to_save = df.select(
            col("courier_id"),
            col("speed").alias("avg_speed"),
            col("order_id"),
            col("value"),
            col("event_time"),
            expr("1").alias("violation_count"), 
            col("event_time").alias("last_violation") 
        )

        try:
            df_to_save.write \
              .format("jdbc") \
              .option("url", "jdbc:postgresql://localhost:5432/logistics_db") \
              .option("dbtable", "public_analytics.courier_performance") \
              .option("user", "admin") \
              .option("password", "password123") \
              .option("driver", "org.postgresql.Driver") \
              .mode("append") \
              .save()
            print("✅ Data saved to Postgres.")
        except Exception as e:
            print(f"❌ DB Error: {e}")

        alerts_list = df.collect()
        for row in alerts_list:
            send_alert_email(row['courier_id'], row['speed'], row['order_id'])

print("🚀 Pipeline is LIVE! Watching for high-speed couriers...")

query = final_alerts.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()