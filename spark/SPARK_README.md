# Spark Streaming & Kafka Integration

This folder contains the Spark Streaming part of the **Real-Time
Customer Churn Analytics** project.

The goal of this part is to connect Spark to Kafka, read the cleaned
customer records produced by NiFi, parse each JSON message using the
shared customer schema, and make the resulting streaming DataFrame
available for the next Spark components.

This README explains the files, the implementation, how to run it, and
how the end-to-end test was performed.

------------------------------------------------------------------------

## 1. Where Spark fits in the project

The complete data flow is:

``` text
Python Producer
      |
      v
Kafka: customer_raw
      |
      v
Apache NiFi
(Cleaning, standardization, validation, schema conversion)
      |
      v
Kafka: customer_processed
      |
      v
Spark Structured Streaming
(Read Kafka messages and parse JSON)
      |
      v
Structured Customer DataFrame
      |
      +------> Data Processing & Feature Engineering
      |
      +------> Churn Analytics
      |
      +------> Database Integration
```

Spark reads from `customer_processed`, not from `customer_raw`. The raw
data is first handled by NiFi, and Spark receives the processed records.

## 2. Technologies used

  -----------------------------------------------------------------------
  Technology                          How it is used
  ----------------------------------- -----------------------------------
  Python                              Writing the Spark application and
                                      the test producer

  PySpark                             Creating the Spark session and
                                      working with streaming DataFrames

  Spark Structured Streaming          Reading customer records
                                      continuously

  Apache Kafka                        Delivering processed customer
                                      messages to Spark

  JSON                                Message format used between Kafka
                                      and Spark
  -----------------------------------------------------------------------

## 3. Project files

  -------------------------------------------------------------------------
  File                                  Purpose
  ------------------------------------- -----------------------------------
  `spark/schemas/customer_schema.py`    Defines the structure and Spark
                                        data types of the customer record

  `spark/ingestion/kafka_consumer.py`   Connects to Kafka, reads the
                                        messages, parses their JSON values,
                                        and returns a streaming DataFrame

  `spark/main.py`                       Creates the Spark session, calls
                                        the ingestion function, and
                                        currently displays the stream in
                                        the console

  `producer/test_producer.py`           Sends one sample customer record to
                                        `customer_raw` to test the pipeline
  -------------------------------------------------------------------------


## 4. Kafka and Spark configuration

The current Kafka source configuration in `kafka_consumer.py` is:

  Setting                      Value
  ---------------------------- --------------------------
  Kafka bootstrap server       `localhost:9092`
  Kafka topic                  `customer_processed`
  Starting offsets             `earliest`
  Message value format         JSON
  Spark application name       `CustomerChurnStreaming`
  Spark SQL session timezone   `UTC`
  Spark log level              `WARN`

The `earliest` option tells the Kafka source where to start when it has
no previously saved streaming offsets. The application was tested
locally using this setting.

## 5. Customer schema

The shared customer schema is defined in:

``` text
spark/schemas/customer_schema.py
```

It contains 23 fields. The schema tells Spark how to interpret each
value when the JSON message is parsed.

  ---------------------------------------------------------------------------
  Field                       Spark type              Description
  --------------------------- ----------------------- -----------------------
  `customer_id`               `StringType`            Customer identifier

  `customer_name`             `StringType`            Customer name

  `age`                       `IntegerType`           Customer age

  `gender`                    `StringType`            Customer gender

  `location`                  `StringType`            Customer location

  `income_level`              `StringType`            Income category

  `signup_date`               `DateType`              Customer signup date

  `churn_date`                `DateType`              Cancellation date, if
                                                      applicable

  `event_timestamp`           `TimestampType`         Time when the event was
                                                      produced

  `service_duration_months`   `IntegerType`           Subscription duration
                                                      in months

  `contract_type`             `StringType`            Customer contract type

  `internet_service`          `StringType`            Internet service type

  `monthly_charges`           `DoubleType`            Monthly charges

  `total_charges`             `DoubleType`            Total charges

  `payment_method`            `StringType`            Payment method

  `calls_count`               `IntegerType`           Number of calls

  `messages_count`            `IntegerType`           Number of messages

  `data_usage_gb`             `DoubleType`            Data usage in GB

  `purchases_count`           `IntegerType`           Number of purchases

  `purchase_frequency`        `DoubleType`            Purchases per month

  `customer_service_calls`    `IntegerType`           Number of customer
                                                      service calls

  `complaints_count`          `IntegerType`           Number of complaints

  `churn_status`              `StringType`            `Yes` or `No`
  ---------------------------------------------------------------------------

All fields are currently declared with `nullable=True` in the PySpark
schema. This allows Spark's schema parser to represent nullable values.
The project validation rules are handled earlier in the NiFi flow and
should remain consistent with the shared data contract.

The `event_timestamp` field uses `TimestampType`. The Spark session
timezone is set to UTC so event-time values are interpreted
consistently.

## 6. How the implementation works

### 6.1 `kafka_consumer.py` --- reading and parsing Kafka messages

This file defines one reusable function:

``` python
read_customer_stream(spark)
```

The function receives an existing Spark session and performs the
ingestion steps:

**Step 1 --- Read from Kafka**

``` python
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customer_processed") \
    .option("startingOffsets", "earliest") \
    .load()
```

`readStream` creates a streaming DataFrame connected to the Kafka topic.
Kafka message values are initially stored as binary data.

**Step 2 --- Convert the message value to a string**

``` python
json_df = kafka_df.select(
    col("value").cast("string").alias("json_data")
)
```

This converts the Kafka `value` column from binary to a string so Spark
can parse its JSON content.

**Step 3 --- Parse the JSON using the customer schema**

``` python
parsed_df = json_df.select(
    from_json(
        col("json_data"),
        customer_schema
    ).alias("customer")
)
```

`from_json()` reads the JSON string and applies the schema defined in
`customer_schema.py`.

**Step 4 --- Select the customer fields**

``` python
customer_df = parsed_df.select("customer.*")
```

This extracts the customer fields from the parsed structure into regular
DataFrame columns.

**Step 5 --- Return the DataFrame**

``` python
return customer_df
```

The function returns the structured streaming DataFrame. It does not
create another Spark session or start a streaming output query. This
separation allows the main application to decide what to do with the
returned DataFrame.

### 6.2 `main.py` --- starting the Spark application

`main.py` is the entry point used to run this Spark application.

It performs the following steps:

1.  Finds the project root and adds it to `sys.path`, so imports such as
    `spark.ingestion.kafka_consumer` work when the application is run
    from the repository root.
2.  Creates a Spark session named `CustomerChurnStreaming`.
3.  Sets the Spark SQL timezone to UTC.
4.  Sets Spark logging to `WARN`.
5.  Calls `read_customer_stream(spark)` and receives `customer_df`.
6.  Starts a console streaming query to display the incoming records.
7.  Uses `awaitTermination()` to keep the application running and
    listening for new messages.

The console output is currently used to test the ingestion pipeline. It
is a temporary output for development; the team can connect the
processing, analytics, and database logic as those components are
integrated.

## 7. Setup and execution

### 7.1 Requirements

Before running the Spark application, make sure that:

-   Python is installed.
-   Apache Spark is installed and `spark-submit` is available in the
    terminal.
-   The project dependencies are installed.
-   Docker Desktop is running if the project services are managed with
    Docker Compose.
-   Kafka is available at `localhost:9092`.
-   NiFi is running and publishing valid records to
    `customer_processed`.

The Kafka connector package used in the successful local test is shown
in the Spark command below.

### 7.2 Start Kafka and NiFi

From the main project directory, start the Docker Compose services:

``` powershell
docker compose up -d
```

Check that the containers are running:

``` powershell
docker ps
```

Make sure the NiFi process group is started and the Kafka topics are
available:

-   `customer_raw`
-   `customer_processed`
-   `customer_invalid`

The producer sends data to `customer_raw`. NiFi consumes and processes
it, then publishes valid records to `customer_processed`.

### 7.3 Run the Spark application

Open a terminal in the **repository root** --- the directory containing
the `spark`, `producer`, and `docker-compose.yml` items.

Run:

``` powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1 spark/main.py
```

This is the connector artifact and version used in the successful local
test. If the installed Spark or Scala version changes, use the matching
Kafka connector package.

When the application starts, Spark reads records from
`customer_processed` and displays the parsed customer columns in the
terminal.

A message such as `only showing top 20 rows` is normal console output
behavior; it does not mean that Spark only reads 20 records.

To stop the streaming application, press:

``` text
Ctrl + C
```

## 8. Testing the complete path

A separate test script is used so the main producer does not need to be
changed just to send one test record.

### 8.1 Start Spark first

Run the Spark application using the command in Section 7.3 and leave
that terminal running.

### 8.2 Send one test record

Open a second terminal in the repository root and run:

``` powershell
python producer/test_producer.py
```

The test script:

1.  Creates a Kafka producer connected to `localhost:9092`.
2.  Builds one sample customer record with the agreed customer fields.
3.  Sets `event_timestamp` to the current UTC time.
4.  Sends the record to `customer_raw`.
5.  Waits for Kafka's send result and prints the topic, partition, and
    offset.
6.  Flushes and closes the producer.

The test record currently uses the customer ID `C99998`.

### 8.3 Expected result

The record should follow this path:

1.  `test_producer.py` sends the record to `customer_raw`.
2.  NiFi consumes and processes the record.
3.  NiFi publishes the valid record to `customer_processed`.
4.  Spark reads the new message.
5.  Spark parses the JSON and displays the customer fields in the
    console.

This end-to-end test was performed successfully. The new test customer
appeared in Spark after passing through the Producer, Kafka, NiFi, and
the processed Kafka topic.

**Testing note:** NiFi uses customer IDs for duplicate detection. If the
same test ID is sent again while it is still present in the duplicate
cache, the record may be routed as a duplicate. Use a new unique
customer ID for another test.

## 9. Work completed in this part

The Spark Streaming & Kafka Integration tasks completed and tested are:

  -----------------------------------------------------------------------
  Task                                What was done
  ----------------------------------- -----------------------------------
  Configure Spark Structured          Created the Spark session and
  Streaming                           configured the streaming
                                      application

  Connect Spark to Kafka              Connected to `localhost:9092` and
                                      subscribed to `customer_processed`

  Read messages continuously          Used `spark.readStream` to read the
                                      Kafka topic

  Parse JSON messages                 Converted the Kafka value to a
                                      string and parsed it with
                                      `from_json()`

  Define the customer schema          Created a 23-field `StructType`
                                      schema with the required Spark data
                                      types

  Create a structured DataFrame       Selected the parsed customer fields
                                      into `customer_df`

  Make the DataFrame available to the Moved the ingestion logic into
  application                         `read_customer_stream(spark)` and
                                      called it from `main.py`
  -----------------------------------------------------------------------

The application was tested with both existing Kafka records and a newly
sent test record. The new record successfully appeared in the Spark
console.

## 10. Handoff to the next project components

The function `read_customer_stream(spark)` returns the structured
streaming DataFrame:

``` python
customer_df = read_customer_stream(spark)
```

This is the input intended for the next Spark components:

  -----------------------------------------------------------------------
  Component                           Responsibility
  ----------------------------------- -----------------------------------
  Data Processing & Feature           Validate and transform the stream,
  Engineering                         create additional features, and
                                      prepare data for analysis

  Churn Analytics                     Calculate churn metrics and analyze
                                      demographic, behavioral, and
                                      time-based patterns

  Database Integration                Store processed customer records
                                      and analytics results
  -----------------------------------------------------------------------

At the moment, `main.py` writes the DataFrame to the console for
testing. The next integration step is to connect the team's processing
and analytics functions to this streaming input and agree on how the
streaming outputs will be stored.

The current ingestion module does not implement churn calculations,
feature engineering, or database writing; these belong to the other
assigned project components.

## 11. Troubleshooting

  -----------------------------------------------------------------------
  Problem                             What to check
  ----------------------------------- -----------------------------------
  Spark cannot connect to Kafka       Check that Kafka is running and
                                      available at `localhost:9092`

  Spark starts but no new records     Check that the NiFi flow is running
  appear                              and publishing to
                                      `customer_processed`

  Customer fields appear as null      Check that the incoming JSON field
                                      names and value types match
                                      `customer_schema.py`

  `ModuleNotFoundError` for `spark`   Run the `spark-submit` command from
                                      the repository root

  A test record is rejected as a      Send the test record with a new
  duplicate                           unique `customer_id`

  Kafka connector package cannot be   Check that the connector version
  resolved                            matches the installed Spark and
                                      Scala versions
  -----------------------------------------------------------------------

## 12. Summary

The Spark ingestion part connects to Kafka's `customer_processed` topic,
continuously reads customer messages, parses their JSON values using the
shared 23-field customer schema, and returns a structured streaming
DataFrame.

The ingestion code is separated into a reusable function and called from
`main.py`. The end-to-end flow was tested successfully with a new
customer record sent through the Producer and NiFi and then received by
Spark.

The DataFrame is now ready to be connected to the team's processing,
churn analytics, and database components.
