# Kafka & Apache NiFi — Customer Churn ETL

## 1. Overview

This module is part of the **Real-Time Customer Churn Analytics** project.

The module implements an ETL pipeline using Apache Kafka and Apache NiFi to consume raw customer records, clean and standardize data, validate data quality, remove duplicate records, and publish structured customer records for downstream Spark processing.

## 2. Objectives

- Consume raw customer records from Kafka.
- Handle missing and inconsistent values.
- Standardize categorical, numerical, and date fields.
- Validate customer records against defined data quality rules.
- Convert valid records into the agreed customer schema.
- Detect and reject duplicate customer records.
- Publish processed and rejected records to separate Kafka topics.
- Provide clean and structured data for Spark Streaming.

## 3. Technologies

| Technology | Purpose |
|---|---|
| Apache Kafka | Real-time data ingestion and message streaming |
| Apache NiFi 2.12.0 | ETL pipeline implementation |
| Apache Avro | Customer schema definition |
| JSON | Data exchange format |
| CSV | Source dataset and location lookup mapping |

## 4. Architecture

```text
CSV Dataset
     |
Python Producer
     |
Kafka: customer_raw
     |
Apache NiFi ETL
     |
     +-- Data Cleaning
     +-- Data Standardization
     +-- Data Validation
     +-- Schema Conversion
     +-- Duplicate Detection
     |
     +-- customer_processed --> Spark Streaming
     |
     +-- customer_invalid --> Rejected Records
```

## 5. Kafka Topics

| Topic | Description |
|---|---|
| `customer_raw` | Receives raw customer records from the Python producer |
| `customer_processed` | Contains cleaned, validated, typed, and deduplicated records |
| `customer_invalid` | Contains records rejected during validation or duplicate detection |

### Kafka Configuration

| Property | Value |
|---|---|
| Bootstrap Servers | `localhost:9092` |
| Consumer Group | `churn-nifi` |
| Auto Offset Reset | `earliest` |
| Security Protocol | `PLAINTEXT` |
| Commit Offsets | `true` |
| Transactions Enabled | `true` |
| Isolation Level | `read_committed` |

## 6. NiFi ETL Pipeline

### 6.1 Data Extraction

**Processor:** `ConsumeKafka`

- Consumes raw customer records from the `customer_raw` topic.
- Uses a dedicated NiFi consumer group.
- Receives customer records in JSON format.

### 6.2 Whitespace Cleaning

**Processor:** `UpdateRecord - Trim Whitespace`

Removes leading and trailing whitespace from customer field values to improve data consistency before subsequent transformations.

### 6.3 Missing Value Normalization

**Processor:** `UpdateRecord - Normalize Missing Values`

Converts common missing-value representations into null values:

- Empty strings.
- Whitespace-only values.
- `N/A`.
- `NaN`.
- `null`.
- `?`.

Missing required values are rejected during validation rather than replaced with artificial values.

### 6.4 Categorical Standardization

**Processor:** `UpdateRecord - Standardize Categories`

Standardizes inconsistent categorical representations, capitalization, abbreviations, and supported spelling errors.

Affected fields:

- `customer_id`
- `gender`
- `income_level`
- `contract_type`
- `internet_service`
- `payment_method`
- `churn_status`

Examples:

| Raw Value | Standardized Value |
|---|---|
| `m` | `Male` |
| `femal` | `Female` |
| `monthly` | `Month-to-Month` |
| `2yr` | `Two Years` |
| `fiber optic` | `Fiber` |
| `cridet card` | `Credit Card` |
| `churned` | `Yes` |
| `active` | `No` |

### 6.5 Location Standardization

**Processor:** `LookupRecord - Standardize Location`

Uses `SimpleCsvFileLookupService` to standardize supported location variations into consistent Egyptian city names.

The lookup mapping is stored in:

```text
nifi/lookups/location_mapping.csv
```

Unrecognized locations are rejected during validation.

### 6.6 Numerical Normalization

**Processor:** `UpdateRecord - Normalize Numerical Values`

Handles supported numerical inconsistencies, including:

- Numerical values containing measurement units.
- Currency suffixes.
- Decimal separator variations.
- Thousands separators.
- Supported written numerical values.
- Service duration conversions from years to months.
- Data usage conversions from MB to GB.
- Supported approximate numerical representations.

Examples:

| Field | Raw Value | Normalized Value |
|---|---|---|
| Age | `30 years` | `30` |
| Service Duration | `2 years` | `24` |
| Monthly Charges | `150 EGP` | `150` |
| Total Charges | `2,500` | `2500` |
| Data Usage | `1500 MB` | `1.5` |
| Customer Service Calls | `three` | `3` |
| Purchase Frequency | `2.5x` | `2.5` |

Invalid numerical representations, including negative zero, are rejected during validation.

### 6.7 Date and Timestamp Normalization

**Processor:** `UpdateRecord - Normalize Date Fields`

Converts supported date and timestamp representations into consistent formats.

| Field | Target Format |
|---|---|
| `signup_date` | `YYYY-MM-DD` |
| `churn_date` | `YYYY-MM-DD` or null |
| `event_timestamp` | ISO 8601 UTC |

Supported transformations include:

- Alternative date separators.
- Textual month representations.
- Unix timestamps in seconds and milliseconds.
- Compact timestamp representations.
- Supported timezone offset conversions.

Timestamps without explicit timezone information are interpreted as UTC.

Unrecoverable date and timestamp values are rejected during validation.

### 6.8 Data Validation

**Processor:** `QueryRecord - Validate Customer Records`

Separates customer records into valid and invalid branches.

The implemented validation rules include:

| Validation Rule | Description |
|---|---|
| Required Fields | Required values must not be missing |
| Customer ID | Must match the expected customer ID format |
| Categorical Values | Must belong to supported categories |
| Age | Must be between 18 and 80 |
| Integer Fields | Must contain valid non-negative integers within the supported range |
| Double Fields | Must contain valid non-negative numerical representations |
| Monthly Charges | Must be greater than zero |
| Numerical Outliers | Configured suspected extreme values are rejected |
| Dates | Must contain valid calendar dates |
| Future Dates | Future calendar dates are rejected |
| Event Timestamp | Must follow the expected UTC timestamp format |
| Chronological Consistency | Dates must follow valid chronological relationships |
| Churn Consistency | Churn status must agree with churn date |

#### Churn Consistency Rules

- `churn_status` must be `Yes` or `No`.
- Churned customers must have a valid churn date.
- Non-churned customers must have a null churn date.
- Churn dates must not precede signup dates.
- Event dates must not precede signup dates.

#### Validation Routing

- Valid records continue to schema conversion.
- Invalid records are routed to `customer_invalid`.

### 6.9 Schema Conversion

**Processor:** `ConvertRecord`

Converts validated customer records into the agreed customer schema using an Avro schema definition.

The output remains JSON, with numerical values converted to their appropriate data types.

The schema is stored in:

```text
nifi/schemas/customer_schema.avsc
```

| Data Type | Number of Fields |
|---|---:|
| String | 11 |
| Integer | 7 |
| Double | 4 |
| Nullable String | 1 |
| **Total** | **23** |

All fields are required except `churn_date`, which supports null values.

### 6.10 Record Splitting

**Processor:** `SplitRecord`

Splits records into individual FlowFiles to support record-level duplicate detection and Kafka publishing.

A separate `SplitRecord` processor is used for validation-rejected records.

### 6.11 Customer ID Extraction

**Processor:** `EvaluateJsonPath`

Extracts the customer ID into a FlowFile attribute.

```text
customer_id = $.customer_id
```

The extracted customer ID is used for duplicate detection and as the Kafka message key for processed records.

### 6.12 Duplicate Detection

**Processor:** `DetectDuplicate`

Detects duplicate records using `customer_id` as the cache entry identifier.

The implemented policy accepts the first valid record encountered for each customer ID.

- Unique customer records continue to the processed topic.
- Subsequent records with the same customer ID are rejected.
- Exact and conflicting duplicates are routed to `customer_invalid`.

Duplicate detection uses `MapCacheClientService` and `MapCacheServer`.

### 6.13 Kafka Publishing

**Processors:**

- `PublishKafka - Processed Records`
- `PublishKafka - Invalid Records`

Successfully processed records are published to `customer_processed`.

Validation-rejected records and duplicate records are published to `customer_invalid`.

Processed Kafka messages use `customer_id` as their message key.

## 7. Controller Services

| Controller Service | Purpose |
|---|---|
| `Kafka3ConnectionService` | Kafka connectivity |
| `JsonTreeReader` | Reading incoming JSON records |
| `JsonRecordSetWriter` | Writing intermediate JSON records |
| `JsonRecordSetWriter - Customer Typed Schema` | Writing records using the agreed schema |
| `SimpleCsvFileLookupService` | Location standardization |
| `MapCacheClientService` | Duplicate cache access |
| `MapCacheServer` | Duplicate identifier storage |

## 8. Project Structure

```text
nifi/
├── flows/
│   └── customer_etl_flow.json
├── lookups/
│   └── location_mapping.csv
├── schemas/
│   └── customer_schema.avsc
└── README.md
```

| File | Description |
|---|---|
| `customer_etl_flow.json` | Exported NiFi process group, processors, connections, and controller services |
| `location_mapping.csv` | Location standardization lookup mapping |
| `customer_schema.avsc` | Agreed customer record schema |
| `README.md` | Implementation documentation and setup instructions |

## 9. Setup and Execution

### 9.1 Prerequisites

- Apache Kafka.
- Apache NiFi 2.12.0.
- Compatible Java installation.
- Python and required producer dependencies.
- Project source dataset.

### 9.2 Start Kafka

From the Kafka installation directory:

```cmd
bin\windows\kafka-server-start.bat config\server.properties
```

Ensure Kafka storage has already been initialized.

### 9.3 Create Kafka Topics

```cmd
bin\windows\kafka-topics.bat --create --if-not-exists --topic customer_raw --bootstrap-server localhost:9092
```

```cmd
bin\windows\kafka-topics.bat --create --if-not-exists --topic customer_processed --bootstrap-server localhost:9092
```

```cmd
bin\windows\kafka-topics.bat --create --if-not-exists --topic customer_invalid --bootstrap-server localhost:9092
```

### 9.4 Import the NiFi Flow

1. Start Apache NiFi.
2. Open `https://localhost:8443/nifi`.
3. Add a Process Group to the canvas.
4. Upload the following flow definition:

```text
nifi/flows/customer_etl_flow.json
```

5. Verify that the process group and controller services are imported successfully.

### 9.5 Configure the Parameter Context

Open the `Customer Churn Config` parameter context.

Update the `PROJECT_ROOT` parameter to the absolute path of the cloned repository on your machine.

Example:

```text
C:/Projects/realtime-customer-churn-analytics
```

The location lookup service uses the following parameterized path:

```text
#{PROJECT_ROOT}/nifi/lookups/location_mapping.csv
```

Verify that Kafka and cache connection settings match your local environment.

### 9.6 Enable Controller Services

1. Enable all required controller services.
2. Verify that all processors have valid configurations.
3. Start the `Customer Churn ETL` process group.

### 9.7 Run the Producer

From the repository root:

```cmd
python producer\producer.py
```

The producer publishes raw customer records to Kafka, where they are consumed and processed by NiFi.

### 9.8 Consume Processed Records

From the Kafka installation directory:

```cmd
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic customer_processed --from-beginning --isolation-level read_committed
```

### 9.9 Consume Rejected Records

```cmd
bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic customer_invalid --from-beginning --isolation-level read_committed
```

### 9.10 Check Consumer Group Status

```cmd
bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group churn-nifi
```

A consumer lag of zero indicates that the consumer group has caught up with the available Kafka messages. NiFi queues should also be checked to confirm processing has completed.

## 10. Testing and Verification

The complete ETL pipeline was tested using the full dataset containing 7,039 customer records.

### 10.1 Record Distribution

| Metric | Result |
|---|---:|
| Total input records | 7,039 |
| Successfully processed records | 3,884 |
| Total rejected records | 3,155 |
| Probable validation rejections | 3,054 |
| Probable exact duplicates | 79 |
| Probable conflicting duplicates | 22 |
| Total probable duplicates | 101 |
| Acceptance rate | 55.18% |
| Rejection rate | 44.82% |
| Record count discrepancy | 0 |

### 10.2 Validation Rejection Analysis

| Rejection Category | Affected Records |
|---|---:|
| Missing required values | 1,453 |
| Invalid numerical format | 806 |
| Churn inconsistency | 381 |
| Invalid timestamps | 373 |
| Invalid categorical values | 176 |
| Chronological inconsistency | 164 |
| Suspected outliers | 145 |
| Invalid dates | 123 |
| Invalid numerical range | 89 |
| Invalid customer ID | 53 |

A single record may violate multiple validation rules. Therefore, the category counts are not mutually exclusive.

### 10.3 Processed Data Quality Verification

| Verification | Result |
|---|---|
| Expected schema fields | 23 |
| Processed records with valid schema | 3,884 |
| Unique processed customer IDs | 3,884 |
| Duplicate IDs in processed output | 0 |
| Processed data quality violations | 0 |
| Total missing required field values among probable validation rejections | 2,056 |
| Total identified validation violations | 4,427 |
| Unexplained probable validation rejections | 0 |
| Rejection explanation coverage | 100% |
| Record reconciliation | Passed |

### 10.4 Integration Testing

The following components were successfully tested:

- Kafka ingestion.
- NiFi data extraction.
- Missing value normalization.
- Categorical standardization.
- Location standardization.
- Numerical normalization.
- Date and timestamp normalization.
- Data validation.
- Schema conversion.
- Duplicate detection.
- Kafka publishing.
- Full dataset processing.
- Processed schema verification.
- Processed data quality verification.
- Rejected record analysis.
- Record count reconciliation.

Temporary Python utilities were used during development to perform data profiling and verify the exported Kafka records. These utilities are not included in the final module.

## 11. Error Handling

The pipeline separates data validation failures from technical processing failures.

| Error Type | Handling |
|---|---|
| Missing required values | Published to `customer_invalid` |
| Invalid categorical values | Published to `customer_invalid` |
| Invalid numerical values | Published to `customer_invalid` |
| Invalid dates and timestamps | Published to `customer_invalid` |
| Duplicate customer records | Published to `customer_invalid` |
| Malformed JSON | Routed to a dedicated failure queue |
| Technical processing failures | Routed to dedicated failure queues |

Malformed JSON handling was tested separately using synthetic records.

## 12. Downstream Integration

The `customer_processed` topic provides cleaned, validated, typed, and deduplicated JSON customer records for Spark Streaming.

Each processed record contains the agreed 23 customer fields.

The downstream modules are responsible for:

- Spark Streaming ingestion.
- Feature engineering.
- Churn analytics.
- Aggregation.
- Database storage.
- Data visualization.

## 13. Conclusion

The Kafka and Apache NiFi ETL module successfully implements real-time customer data ingestion, cleaning, standardization, validation, schema conversion, duplicate detection, and Kafka publishing.

Full dataset testing successfully reconciled all 7,039 input records, producing 3,884 processed records and 3,155 rejected records.

All processed records passed the implemented schema and data quality verification checks.