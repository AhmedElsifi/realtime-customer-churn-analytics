# Architecture — Customer Churn Analytics

Real-time Big Data pipeline that generates customer records, transforms them with NiFi,
analyzes them with Spark Structured Streaming, and stores results in MongoDB.

## High-level data flow

```mermaid
flowchart LR
    A["Python Generator<br/>+ Kafka Producer<br/>(Member 1)"]
    B[("Kafka<br/>customer_raw")]
    C["Apache NiFi<br/>ETL<br/>(Member 2)"]
    D[("Kafka<br/>customer_processed")]
    X[("Kafka<br/>customer_invalid")]
    E["Spark Streaming<br/>Kafka ingestion<br/>(Member 3)"]
    F["PySpark Processing &<br/>Feature Engineering<br/>(Ahmed)"]
    G["Churn Analytics<br/>(Member 4)"]
    H[("MongoDB<br/>customers")]
    I[("MongoDB<br/>churn_analytics<br/>churn_time_windows")]
    J["Database Integration<br/>(Ali)"]

    A -->|JSON records| B
    B --> C
    C -->|valid records| D
    C -->|rejected records| X
    D --> E --> F --> G
    F -->|enriched records| H
    G -->|aggregated results| I
    H --> J
    I --> J
```

