
from pyspark.sql.functions import col, from_json
from spark.schemas.customer_schema import customer_schema


def read_customer_stream(spark):

    # Read streaming data from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "customer_processed") \
        .option("startingOffsets", "earliest") \
        .load()

    # Convert Kafka value from binary to string
    json_df = kafka_df.select(
        col("value").cast("string").alias("json_data")
    )

    # Parse JSON using customer schema
    parsed_df = json_df.select(
        from_json(
            col("json_data"),
            customer_schema
        ).alias("customer")
    )

    # Extract customer columns
    customer_df = parsed_df.select("customer.*")

    return customer_df
