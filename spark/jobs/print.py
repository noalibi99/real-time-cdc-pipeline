"""
Print Kafka messages to console using Spark Structured Streaming.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "trading.public.trades"
SPARK_VERSION = "3.5.5"
SCALA_VERSION = "2.12"

KAFKA_PACKAGE = (
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
)

# Ensure the Kafka connector JAR is available even outside spark-submit.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    f"--packages {KAFKA_PACKAGE} --conf spark.jars.ivy=/tmp/.ivy2 pyspark-shell",
)

spark = (
    SparkSession.builder
    .appName("kafka-print")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .config("spark.jars.ivy", "/tmp/.ivy2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

messages_df = raw_df.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
)

query = (
    messages_df.writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("append")
    .start()
)

query.awaitTermination()
