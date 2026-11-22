from kafka import KafkaProducer
from time import sleep
from json import dumps
import csv

topic_name = "customer_raw"
bootstrap_servers = ["localhost:9092"]  

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: dumps(x, ensure_ascii=False).encode("utf-8"),
    acks="all",   
)


from pathlib import Path
file_name = Path(__file__).resolve().parent.parent / "data" / "raw" / "churn_dataset.csv"

# ---------------- Streaming configuration ----------------
batch_size = 10   
delay = 1        

sent_records = 0
batch_count = 0

print("Starting to send customer records to Kafka...")

try:
    
    with open(file_name, mode="r", encoding="utf-8-sig", newline="") as file:

        csv_reader = csv.DictReader(file)

        for customer in csv_reader:

           
            producer.send(topic_name, value=customer)

            sent_records += 1
            batch_count += 1

            if batch_count == batch_size:
                producer.flush()
                print(f"Sent {sent_records} customer records")
                batch_count = 0
                sleep(delay)

except KeyboardInterrupt:
    print("\nStreaming stopped by user.")

finally:
   
    producer.flush()
    producer.close()

print("--------------------------------")
print(f"Total records sent: {sent_records}")
print("Data publishing completed!")
print("--------------------------------")
