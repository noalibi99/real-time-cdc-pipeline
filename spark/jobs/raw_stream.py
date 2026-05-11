import os

from pyspark.sql import SparkSession

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "trading.public.trades")

spark = SparkSession.builder \
    .appName("kafka-raw-stream") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

raw = df.selectExpr("CAST(value AS STRING)")

query = raw.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()