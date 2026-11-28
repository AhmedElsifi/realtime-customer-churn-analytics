
from kafka import KafkaProducer
from json import dumps
from datetime import datetime, timezone


# 1. Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(
        x, ensure_ascii=False
    ).encode("utf-8"),
    acks="all"
)


# 2. Create one test customer
test_customer = {
    "customer_id": "C99998",
    "customer_name": "Test Customer",
    "age": 30,
    "gender": "Female",
    "location": "Cairo",
    "income_level": "Medium",

    "signup_date": "2025-01-10",
    "service_duration_months": 10,

    "contract_type": "Month-to-Month",
    "internet_service": "Fiber",

    "monthly_charges": 250.0,
    "total_charges": 2500.0,
    "payment_method": "Cash",

    "calls_count": 50,
    "messages_count": 80,
    "data_usage_gb": 15.5,
    "purchases_count": 2,
    "purchase_frequency": 0.2,

    "customer_service_calls": 1,
    "complaints_count": 0,

    "churn_status": "No",
    "churn_date": None,

    "event_timestamp": datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
}


# 3. Send the test record to Kafka
try:
    future = producer.send(
        "customer_raw",
        value=test_customer
    )

    metadata = future.get(timeout=10)

    print("Test record sent successfully!")
    print(f"Customer ID: {test_customer['customer_id']}")
    print(f"Topic: {metadata.topic}")
    print(f"Partition: {metadata.partition}")
    print(f"Offset: {metadata.offset}")
    print(f"Event Timestamp: {test_customer['event_timestamp']}")

finally:
    producer.flush()
    producer.close()
