import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trading.public.trades")
CHECKPOINT_PATH = os.getenv(
    "BRONZE_CHECKPOINT_PATH", "s3a://bronze/checkpoints/bronze_trades"
)
BRONZE_TABLE = os.getenv("BRONZE_TABLE", "rest_catalog.bronze.trades")
BRONZE_TABLE_LOCATION = os.getenv("BRONZE_TABLE_LOCATION", "s3a://bronze/trades")
BRONZE_NAMESPACE_LOCATION = os.getenv(
    "BRONZE_NAMESPACE_LOCATION", "s3a://bronze/"
)

REST_CATALOG_URI = os.getenv("REST_CATALOG_URI", "http://rest:8181")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password")

spark = (
    SparkSession.builder
    .appName("trades-bronze")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.rest_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.rest_catalog.type", "rest")
    .config("spark.sql.catalog.rest_catalog.uri", REST_CATALOG_URI)
    .config("spark.sql.catalog.rest_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.rest_catalog.s3.endpoint", S3_ENDPOINT)
    .config("spark.sql.catalog.rest_catalog.s3.access-key-id", S3_ACCESS_KEY)
    .config("spark.sql.catalog.rest_catalog.s3.secret-access-key", S3_SECRET_KEY)
    .config("spark.sql.catalog.rest_catalog.s3.path-style-access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Create the bronze namespace and keep the raw message envelope intact.
spark.sql(
    "CREATE NAMESPACE IF NOT EXISTS rest_catalog.bronze "
    f"LOCATION '{BRONZE_NAMESPACE_LOCATION}'"
)

# Explicitly set the bronze table location in the bronze bucket.
create_table_sql = (
    "CREATE TABLE rest_catalog.bronze.trades ("
    "key BINARY, "
    "value BINARY, "
    "topic STRING, "
    "partition INT, "
    "offset BIGINT, "
    "timestamp TIMESTAMP, "
    "timestampType INT, "
    "headers ARRAY<STRUCT<key:STRING, value:BINARY>>, "
    "ticker STRING"
    ") USING iceberg "
    "PARTITIONED BY (ticker) "
    f"LOCATION '{BRONZE_TABLE_LOCATION}'"
)

try:
    spark.sql(create_table_sql)
except Exception:
    try:
        spark.sql("CALL rest_catalog.system.drop_table('bronze', 'trades', true)")
    except Exception:
        pass
    spark.sql("DROP TABLE IF EXISTS rest_catalog.bronze.trades")
    spark.sql(create_table_sql)

raw_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

raw_kafka_df = raw_kafka_df.withColumn(
    "ticker", get_json_object(col("value").cast("string"), "$.payload.ticker")
)

bronze_query = (
    raw_kafka_df.writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .toTable(BRONZE_TABLE)
)

bronze_query.awaitTermination()