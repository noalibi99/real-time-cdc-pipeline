import os
from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "trading.public.trades")

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
    "org.apache.kafka:kafka-clients:3.4.1",
    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.5",
    "org.apache.commons:commons-pool2:2.11.1",
])

spark = (
    SparkSession.builder
    .appName("kafka-raw-stream")
    .config("spark.jars.ivy", "/tmp/.ivy2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"[INFO] Spark {spark.version} started")
print(f"[INFO] Broker : {KAFKA_BOOTSTRAP}")
print(f"[INFO] Topic  : {KAFKA_TOPIC}")

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr(
        "CAST(key   AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "topic",
        "partition",
        "offset",
        "timestamp",
    )
)

count = df.count()
print(f"[INFO] Messages read: {count}")
df.show(20, truncate=False)

spark.stop()