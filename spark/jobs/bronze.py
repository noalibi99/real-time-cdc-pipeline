"""
Bronze streaming job: Kafka raw trades -> Iceberg.

This job intentionally keeps the Kafka payload opaque for zero-loss ingestion.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp, get_json_object, upper


ICEBERG_CATALOG = "rest_catalog"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "trading.public.trades"
BRONZE_TABLE = f"{ICEBERG_CATALOG}.bronze.trades"
BRONZE_TABLE_LOCATION = "s3://bronze/trades"
BRONZE_DATA_LOCATION = "s3://bronze/trades/data"
BRONZE_CHECKPOINT = "s3a://bronze/checkpoints/bronze_trades"

SPARK_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    ]
)

os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    f"--packages {SPARK_PACKAGES} --conf spark.jars.ivy=/tmp/.ivy2 pyspark-shell",
)


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.uri", "http://rest:8181")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", "s3://warehouse/")
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint",
            "http://minio:9000",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def ensure_iceberg_objects(spark: SparkSession) -> None:
    # Explicitly maps the bronze namespace to its dedicated MinIO bucket.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest_catalog.bronze LOCATION 's3://bronze/'")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS rest_catalog.bronze.trades (
            key STRING,
            value STRING,
            ingest_timestamp TIMESTAMP,
            ticker STRING
        )
        USING iceberg
        PARTITIONED BY (ticker)
        LOCATION '{BRONZE_TABLE_LOCATION}'
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    try:
        spark.sql(f"ALTER TABLE {BRONZE_TABLE} ADD COLUMNS (ticker STRING)")
    except Exception:
        pass
    try:
        spark.sql(f"ALTER TABLE {BRONZE_TABLE} DROP PARTITION FIELD days(ingest_timestamp)")
    except Exception:
        pass
    try:
        spark.sql(f"ALTER TABLE {BRONZE_TABLE} ADD PARTITION FIELD ticker")
    except Exception:
        pass
    spark.sql(
        f"""
        ALTER TABLE {BRONZE_TABLE}
        SET TBLPROPERTIES ('write.data.path' = '{BRONZE_DATA_LOCATION}')
        """
    )


def main() -> None:
    spark = build_spark("medallion-bronze-trades")
    spark.sparkContext.setLogLevel("WARN")
    ensure_iceberg_objects(spark)

    raw_trades = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze_trades = raw_trades.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
        current_timestamp().alias("ingest_timestamp"),
        upper(
            coalesce(
                get_json_object(col("value").cast("string"), "$.ticker"),
                get_json_object(col("value").cast("string"), "$.payload.ticker"),
                get_json_object(col("value").cast("string"), "$.after.ticker"),
                get_json_object(col("value").cast("string"), "$.payload.after.ticker"),
            )
        ).alias("ticker"),
    )

    query = (
        bronze_trades.writeStream
        .format("iceberg")
        .outputMode("append")
        # Streaming checkpoint lives in the physically isolated bronze bucket.
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        # Iceberg destination for the Bronze layer.
        .toTable(BRONZE_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
