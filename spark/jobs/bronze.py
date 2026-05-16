from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
import time

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "trading.public.trades"
BRONZE_PATH = "s3a://bronze/trades"
CHECKPOINT_PATH = "s3a://bronze/checkpoints/trades"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

spark = SparkSession.builder \
    .appName("trades-bronze") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

payload_schema = StructType([
    StructField("trade_id",        StringType(),  True),  # relaxed to True for debugging
    StructField("portfolio_id",    StringType(),  True),
    StructField("ticker",          StringType(),  True),
    StructField("side",            StringType(),  True),
    StructField("quantity",        DoubleType(),  True),
    StructField("price",           DoubleType(),  True),
    StructField("trade_value",     DoubleType(),  True),
    StructField("fees",            DoubleType(),  True),
    StructField("trade_timestamp", LongType(),    True),
    StructField("__deleted",       StringType(),  True),
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Step 1: cast raw bytes to string
parsed_df = raw_df.withColumn("raw_json", col("value").cast("string"))

# Step 2: extract payload JSON string from envelope
parsed_df = parsed_df.withColumn("payload_json", get_json_object(col("raw_json"), "$.payload"))

# Step 3: parse payload into struct
parsed_df = parsed_df.withColumn("payload", from_json(col("payload_json"), payload_schema))

# Step 4: select final columns, drop nulls on trade_id to filter bad rows
trades_df = parsed_df \
    .select("payload.*") \
    .filter(col("trade_id").isNotNull())

# --- Debug sink: print raw JSON to confirm Kafka is readable ---
raw_query = parsed_df \
    .select("raw_json", "payload_json") \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .trigger(processingTime="30 seconds") \
    .start()

# --- Debug sink: print parsed trades ---
console_query = trades_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 5) \
    .trigger(processingTime="30 seconds") \
    .start()

# --- Main sink: write to MinIO ---
minio_query = trades_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("path", BRONZE_PATH) \
    .partitionBy("ticker") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

# --- Progress monitor ---
while minio_query.isActive:
    progress = minio_query.lastProgress
    if progress:
        print("=== Stream Progress ===")
        print(f"  numInputRows    : {progress['numInputRows']}")
        print(f"  inputRowsPerSec : {progress['inputRowsPerSecond']}")
        print(f"  sources         : {progress['sources']}")
        print(f"  sink            : {progress['sink']}")
        if progress['numInputRows'] == 0:
            print("  WARNING: No rows read from Kafka")
        elif progress['numInputRows'] > 0:
            print("  OK: rows are being read")
    else:
        print("Waiting for first batch...")
    time.sleep(15)