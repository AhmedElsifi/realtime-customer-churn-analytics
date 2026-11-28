
import sys
from pathlib import Path

# Get project root automatically
PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession
from spark.ingestion.kafka_consumer import read_customer_stream


# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("CustomerChurnStreaming") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# 2. Get structured customer DataFrame
customer_df = read_customer_stream(spark)


# 3. Display streaming data (temporary testing)
query = customer_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
