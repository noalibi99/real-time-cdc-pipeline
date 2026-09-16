"""
Gold streaming job: Silver trades -> portfolio positions.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum, when


ICEBERG_CATALOG = "rest_catalog"
SILVER_TABLE = f"{ICEBERG_CATALOG}.silver.trades"
GOLD_TABLE = f"{ICEBERG_CATALOG}.gold.portfolio_positions"
GOLD_TABLE_LOCATION = "s3://gold/portfolio_positions"
GOLD_DATA_LOCATION = "s3://gold/portfolio_positions/data"
GOLD_CHECKPOINT = "s3a://gold/checkpoints/gold_positions_v2"

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
    # Explicitly maps the gold namespace to its dedicated MinIO bucket.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest_catalog.gold LOCATION 's3://gold/'")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS rest_catalog.gold.portfolio_positions (
            portfolio_id STRING,
            ticker STRING,
            net_quantity DECIMAL(28,2),
            total_fees DECIMAL(28,2),
            total_value DECIMAL(28,2)
        )
        USING iceberg
        PARTITIONED BY (ticker)
        LOCATION '{GOLD_TABLE_LOCATION}'
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    try:
        spark.sql(f"ALTER TABLE {GOLD_TABLE} ADD COLUMN total_value DECIMAL(28,2)")
    except Exception:
        pass
    try:
        spark.sql(f"ALTER TABLE {GOLD_TABLE} ADD PARTITION FIELD ticker")
    except Exception:
        pass
    spark.sql(
        f"""
        ALTER TABLE {GOLD_TABLE}
        SET TBLPROPERTIES ('write.data.path' = '{GOLD_DATA_LOCATION}')
        """
    )

def drop_gold_table(spark: SparkSession) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE}")


def overwrite_gold_table(batch_df, _batch_id: int) -> None:
    # Iceberg destination for the Gold layer: replace current positions each batch.
    batch_df.writeTo(GOLD_TABLE).overwrite(lit(True))


def main() -> None:
    spark = build_spark("medallion-gold-portfolio-positions")
    spark.sparkContext.setLogLevel("WARN")
    # drop_gold_table(spark)
    ensure_iceberg_objects(spark)

    silver_trades = spark.readStream.format("iceberg").load(SILVER_TABLE)

    signed_quantity = (
        when(col("side") == "BUY", col("quantity"))
        .when(col("side") == "SELL", -col("quantity"))
        .otherwise(lit(0).cast("decimal(18,2)"))
    )

    portfolio_positions = (
        silver_trades
        # Watermark bounds aggregation state for real-time portfolio positions.
        .withWatermark("trade_timestamp", "15 minutes")
        .groupBy("portfolio_id", "ticker")
        .agg(
            spark_sum(signed_quantity).alias("net_quantity"),
            spark_sum(col("fees")).alias("total_fees"),
            spark_sum(col("trade_value").cast("decimal(18,2)")).alias("total_value"),
        )
    )

    query = (
        portfolio_positions.writeStream
        # Complete mode is required for running streaming aggregations.
        .outputMode("complete")
        # Streaming checkpoint lives in the physically isolated gold bucket.
        .option("checkpointLocation", GOLD_CHECKPOINT)
        .foreachBatch(overwrite_gold_table)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
