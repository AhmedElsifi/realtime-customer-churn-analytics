## Team Responsibilities

| Member | Responsibility | Implementation |
| --- | --- | --- |
| Shahd Elghandour | Data Source & Kafka Producer | Generate customer data and publish it to the raw Kafka topic. |
| Hour | Kafka & NiFi ETL | Configure Kafka topics and implement the NiFi ETL workflow. |
| Shahd Ahmed | Spark Streaming & Kafka Integration | Connect Spark to Kafka, consume processed messages, and implement the streaming input pipeline. |
| Ahmed | PySpark Data Processing & Feature Engineering | Implement customer data processing, validation, feature extraction, and analytical data preparation in PySpark. |
| Jana | Churn Analytics | Implement churn-rate calculations and demographic/behavioral analytics using Spark. |
| Ali | Database Integration | Design the database and implement saving Spark outputs into it. |

## Shahd Elghandour **— Data Source & Kafka Producer**

Data generation and ingestion

**Main objective:** Generate customer data and send it to Kafka.

### Tasks

1. Prepare customer records containing fields such as:
    - Customer ID
    - Age, gender, location, and income level
    - Service usage
    - Purchase history and purchase frequency
    - Monthly charges and payment method
    - Customer service calls
    - Churn status and churn date
    - Event timestamp
2. Create a Python script that generates or reads customer records.
3. Configure a Kafka producer to publish these records.
4. Send records gradually to simulate a real-time data stream.
5. Publish messages to the raw Kafka topic, for example `customer_raw`.

### **Expected output**

A continuously populated Kafka topic containing raw customer records.

**Understand:** Python data generation, JSON, Kafka producers, topics, and message publishing.

## Hour **— Kafka & NiFi ETL**

Data ingestion and ETL

**Main objective:** Receive raw customer data, transform it using NiFi, and publish the transformed data.

### Tasks

1. Configure the Kafka raw topic if it hasn't already been configured.
2. Configure NiFi to consume messages from `customer_raw`.
3. Build a NiFi flow to:
    - Read incoming JSON records.
    - Validate required fields.
    - Handle missing or malformed data.
    - Standardize values and formats.
    - Transform the records into the agreed schema.
4. Configure NiFi to publish transformed records to another Kafka topic, such as `customer_processed`.

### **Expected output**

A NiFi ETL workflow and a populated `customer_processed` Kafka topic.

**Understand:** Kafka consumers and producers, NiFi processors, connections, data flow, and ETL.

## Shahd Ahmed **— Spark Streaming & Kafka Integration**

Streaming ingestion

**Main objective:** Connect Spark to Kafka and continuously receive processed customer data.

### Tasks

1. Configure the Spark Structured Streaming application.
2. Connect Spark to Kafka's `customer_processed` topic.
3. Read incoming messages continuously.
4. Parse the Kafka message values from JSON.
5. Define the customer data schema, including the event timestamp as a timestamp type.
6. Convert the incoming stream into a structured Spark DataFrame.
7. Make the resulting streaming DataFrame available to the processing and analytics logic.

### **Expected output**

A working Spark Structured Streaming application that continuously reads structured customer records from Kafka.

**Understand:** Spark, Structured Streaming, Kafka integration, schemas, DataFrames, and streaming queries.

## Ahmed **— PySpark Data Processing & Feature Engineering**

Spark implementation

**Main objective:** Implement the processing and enrichment logic between streaming ingestion and churn analytics.

This is your own technical component, not a supervision assignment.

### Tasks

**1. Data validation**

Although NiFi handles initial ETL, implement additional checks in Spark to ensure the data is usable.

Examples:

- Customer ID must not be empty.
- Age must be within an acceptable range.
- Monthly charges must be numeric.
- Churn status must be valid.
- Event timestamp must be a valid timestamp.

**2. Data transformation**

Prepare the incoming data for analysis:

- Convert age to an integer.
- Convert monthly charges to numeric values.
- Normalize categorical values.
- Handle any remaining null values according to agreed rules.

**3. Feature engineering**

Create additional analytical fields from existing customer attributes.

For example:

| Original field | New feature |
| --- | --- |
| Age = 23 | Age group = 18–25 |
| Monthly charges = 150 | Charge category = High |
| Support calls = 6 | Support category = Frequent |
| Data usage = 40 GB | Usage category = High |
| Purchases count / frequency | Purchase category = Low / Medium / High |

**4. Prepare the analytical DataFrame**

Create the cleaned and enriched DataFrame that Jana will use for churn calculations.

**5. Implement reusable PySpark functions**

Keep the processing logic organized into functions so the rest of the Spark application can use it.

### **Expected output**

A PySpark processing module that receives the streaming customer data and produces a validated, transformed, enriched DataFrame.

**Understand:** PySpark DataFrames, transformations, filtering, conditional columns, data types, null handling, and feature engineering.

## **Jana — Churn Analytics**

Data analysis

**Main objective:** Analyze customer churn using Spark.

### Tasks

Use the prepared customer data to calculate meaningful analytics.

For example:

1. Calculate total customers.
2. Count active and churned customers.
3. Calculate the overall churn rate.
4. Analyze churn by age group.
5. Analyze churn by location.
6. Investigate whether frequent customer support calls are associated with higher churn.
7. Analyze churn according to monthly charges or payment method.
8. Analyze churn by gender and income level.
9. Analyze churn by usage and purchase behavior (purchase history and frequency).
10. Analyze customer service calls over time using Spark `window()` on the event timestamp with a watermark.

Example calculation:

$$
Churn\ Rate = \frac{Churned\ Customers}{Total\ Customers}\times100
$$

The member should implement these calculations using Spark DataFrame operations and the streaming processing approach agreed upon by the team.

### **Expected output**

Spark analytics results, such as churn rates, customer counts, grouped statistics, and windowed statistics.

**Understand:** PySpark DataFrame operations, aggregations, grouping, filtering, windows, watermarks, and streaming analytics.

## **Ali — Database Integration**

Data persistence

**Main objective:** Store processed customer information and analytics results in a database.

### Tasks

1. Choose and configure the agreed database.
2. Design the necessary collections or tables.
3. Connect the Spark application to the database.
4. Implement writing customer records into the database.
5. Implement storing analytics results.
6. Ensure that new streaming results can be saved without accidentally duplicating records.
7. Verify that stored information can be retrieved.

For example, if the team chooses MongoDB, the database could contain:

- `customers`: processed customer records.
- `churn_analytics`: calculated churn statistics.

### **Expected output**

A working database containing customer information and churn analytics.

**Understand:** Database design, database connections, Spark output operations, and data persistence.

All team members are expected to understand the overall architecture, the complete data flow, and their respective contributions.