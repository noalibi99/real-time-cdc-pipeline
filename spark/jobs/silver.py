"""
Silver streaming job: Bronze raw trades -> cleaned Iceberg trades.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, expr, from_json, upper
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ICEBERG_CATALOG = "rest_catalog"
BRONZE_TABLE = f"{ICEBERG_CATALOG}.bronze.trades"
SILVER_TABLE = f"{ICEBERG_CATALOG}.silver.trades"
SILVER_TABLE_LOCATION = "s3://silver/trades"
SILVER_DATA_LOCATION = "s3://silver/trades/data"
SILVER_CHECKPOINT = "s3a://silver/checkpoints/silver_trades"

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


TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", StringType(), True),
        StructField("portfolio_id", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("side", StringType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("price", DoubleType(), True),
        StructField("fees", DoubleType(), True),
        StructField("trade_timestamp", StringType(), True),
    ]
)

DEBEZIUM_ENVELOPE_SCHEMA = StructType(
    [
        StructField("payload", TRADE_SCHEMA, True),
    ]
)


def ensure_iceberg_objects(spark: SparkSession) -> None:
    # Explicitly maps the silver namespace to its dedicated MinIO bucket.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest_catalog.silver LOCATION 's3://silver/'")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS rest_catalog.silver.trades (
            trade_id STRING,
            portfolio_id STRING,
            ticker STRING,
            side STRING,
            quantity DECIMAL(18,2),
            price DECIMAL(18,2),
            fees DECIMAL(18,2),
            trade_timestamp TIMESTAMP,
            trade_value DECIMAL(38,4)
        )
        USING iceberg
        PARTITIONED BY (ticker)
        LOCATION '{SILVER_TABLE_LOCATION}'
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    try:
        spark.sql(f"ALTER TABLE {SILVER_TABLE} DROP PARTITION FIELD days(trade_timestamp)")
    except Exception:
        pass
    try:
        spark.sql(f"ALTER TABLE {SILVER_TABLE} ADD PARTITION FIELD ticker")
    except Exception:
        pass
    spark.sql(
        f"""
        ALTER TABLE {SILVER_TABLE}
        SET TBLPROPERTIES ('write.data.path' = '{SILVER_DATA_LOCATION}')
        """
    )


def main() -> None:
    spark = build_spark("medallion-silver-trades")
    spark.sparkContext.setLogLevel("WARN")
    ensure_iceberg_objects(spark)

    bronze_trades = spark.readStream.format("iceberg").load(BRONZE_TABLE)

    parsed_trades = (
        bronze_trades
        .select(
            from_json(col("value"), TRADE_SCHEMA).alias("flat_trade"),
            from_json(col("value"), DEBEZIUM_ENVELOPE_SCHEMA).alias("debezium_trade"),
        )
        .select(
            coalesce(
                col("flat_trade.trade_id"),
                col("debezium_trade.payload.trade_id"),
            ).alias("trade_id"),
            coalesce(
                col("flat_trade.portfolio_id"),
                col("debezium_trade.payload.portfolio_id"),
            ).alias("portfolio_id"),
            coalesce(
                col("flat_trade.ticker"),
                col("debezium_trade.payload.ticker"),
            ).alias("ticker"),
            coalesce(
                col("flat_trade.side"),
                col("debezium_trade.payload.side"),
            ).alias("side"),
            coalesce(
                col("flat_trade.quantity"),
                col("debezium_trade.payload.quantity"),
            ).alias("quantity"),
            coalesce(
                col("flat_trade.price"),
                col("debezium_trade.payload.price"),
            ).alias("price"),
            coalesce(
                col("flat_trade.fees"),
                col("debezium_trade.payload.fees"),
            ).alias("fees"),
            coalesce(
                col("flat_trade.trade_timestamp"),
                col("debezium_trade.payload.trade_timestamp"),
            ).alias("trade_timestamp_raw"),
        )
    )

    cleaned_trades = (
        parsed_trades
        .select(
            col("trade_id"),
            col("portfolio_id"),
            upper(col("ticker")).alias("ticker"),
            upper(col("side")).alias("side"),
            col("quantity").cast("decimal(18,2)").alias("quantity"),
            col("price").cast("decimal(18,2)").alias("price"),
            col("fees").cast("decimal(18,2)").alias("fees"),
            expr(
                """
                CASE
                  WHEN trade_timestamp_raw RLIKE '^[0-9]+$'
                    THEN timestamp_micros(CAST(trade_timestamp_raw AS BIGINT))
                  ELSE CAST(trade_timestamp_raw AS TIMESTAMP)
                END
                """
            ).cast(TimestampType()).alias("trade_timestamp"),
        )
        .withColumn("trade_value", col("quantity") * col("price"))
        # Data quality filter: keep rows with usable identifiers and positive size.
        .where((col("trade_id").isNotNull()) & (col("quantity") > 0))
        # Watermark bounds streaming state, then trade_id-level deduplication.
        .withWatermark("trade_timestamp", "2 hours")
        .dropDuplicates(["trade_id"])
    )

    query = (
        cleaned_trades.writeStream
        .format("iceberg")
        .outputMode("append")
        # Streaming checkpoint lives in the physically isolated silver bucket.
        .option("checkpointLocation", SILVER_CHECKPOINT)
        # Iceberg destination for the Silver layer.
        .toTable(SILVER_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
