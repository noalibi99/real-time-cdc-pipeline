from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("trading-stream") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trading.public.trades") \
    .option("startingOffsets", "latest") \
    .load()

raw = df.selectExpr("CAST(value AS STRING)")

query = raw.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()