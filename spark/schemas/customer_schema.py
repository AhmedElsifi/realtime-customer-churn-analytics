
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType
)

customer_schema = StructType([

    # Customer Information
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("location", StringType(), True),
    StructField("income_level", StringType(), True),

    # Dates
    StructField("signup_date", DateType(), True),
    StructField("churn_date", DateType(), True),
    StructField("event_timestamp", TimestampType(), True),

    # Subscription Information
    StructField("service_duration_months", IntegerType(), True),
    StructField("contract_type", StringType(), True),
    StructField("internet_service", StringType(), True),

    # Financial Information
    StructField("monthly_charges", DoubleType(), True),
    StructField("total_charges", DoubleType(), True),
    StructField("payment_method", StringType(), True),

    # Customer Activity
    StructField("calls_count", IntegerType(), True),
    StructField("messages_count", IntegerType(), True),
    StructField("data_usage_gb", DoubleType(), True),
    StructField("purchases_count", IntegerType(), True),
    StructField("purchase_frequency", DoubleType(), True),
    StructField("customer_service_calls", IntegerType(), True),
    StructField("complaints_count", IntegerType(), True),

    # Churn Information
    StructField("churn_status", StringType(), True)

])
