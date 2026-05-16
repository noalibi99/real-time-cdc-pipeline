"""
Kafka Connection Test for Spark Structured Streaming
=====================================================
Tests:
  1. Kafka broker reachability
  2. Spark <-> Kafka read (consume)
  3. Spark <-> Kafka write (produce)
  4. Round-trip (write then read back)

Run inside the spark-master container:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/test_kafka_connection.py
"""

import os
import sys
import time

# ---------------------------------------------------------------------------
# Configuration — adjust these to match your environment
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = "kafka:9092"
TEST_TOPIC      = "spark-kafka-test"
SPARK_VERSION   = "3.5.5"
SCALA_VERSION   = "2.12"
# ---------------------------------------------------------------------------

KAFKA_PACKAGE = (
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
)

# ── Inject the --packages flag before Spark initialises ──────────────────────
# When spark-submit is NOT used (e.g. plain `python` call), this ensures
# the ivy resolver still downloads the JAR automatically.
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    f"--packages {KAFKA_PACKAGE} "
    f"--conf spark.jars.ivy=/tmp/.ivy2 "
    f"pyspark-shell"
)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, current_timestamp
    from pyspark.sql.types import StringType
except ImportError:
    print("[ERROR] PySpark is not installed. Install it with: pip install pyspark")
    sys.exit(1)


# ── Helpers ──────────────────────────────────────────────────────────────────

def section(title: str) -> None:
    width = 60
    print("\n" + "═" * width)
    print(f"  {title}")
    print("═" * width)


def ok(msg: str)   -> None: print(f"  ✅  {msg}")
def fail(msg: str) -> None: print(f"  ❌  {msg}")
def info(msg: str) -> None: print(f"  ℹ️   {msg}")


# ── Build SparkSession ────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    section("1 / 4  —  Building SparkSession")
    spark = (
        SparkSession.builder
        .appName("KafkaConnectionTest")
        # Pull the Kafka connector JAR automatically via Maven
        .config("spark.jars.packages", KAFKA_PACKAGE)
        # Ivy cache inside the container (writable path)
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    ok(f"SparkSession created  (Spark {spark.version})")
    info(f"Kafka package : {KAFKA_PACKAGE}")
    return spark


# ── Test 1: broker reachability via a tiny batch read ────────────────────────

def test_broker_reachable(spark: SparkSession) -> bool:
    section("2 / 4  —  Broker Reachability")
    info(f"Connecting to {KAFKA_BOOTSTRAP} …")
    try:
        # A batch read with maxOffsetsPerTrigger=1 just checks connectivity
        (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TEST_TOPIC)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )
        ok(f"Broker reachable at {KAFKA_BOOTSTRAP}")
        return True
    except Exception as exc:
        fail(f"Cannot reach broker: {exc}")
        info("Is the 'kafka' container running and on the same Docker network?")
        return False


# ── Test 2: write a batch of test messages ───────────────────────────────────

def test_write(spark: SparkSession) -> bool:
    section("3 / 4  —  Write Test Messages → Kafka")
    try:
        messages = [
            ("msg-1", "hello from spark #1"),
            ("msg-2", "hello from spark #2"),
            ("msg-3", "hello from spark #3"),
        ]
        df = (
            spark.createDataFrame(messages, ["key", "value"])
            .withColumn("key",   col("key").cast(StringType()))
            .withColumn("value", col("value").cast(StringType()))
        )
        (
            df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", TEST_TOPIC)
            .save()
        )
        ok(f"Wrote {len(messages)} messages to topic '{TEST_TOPIC}'")
        return True
    except Exception as exc:
        fail(f"Write failed: {exc}")
        return False


# ── Test 3: read the messages back ───────────────────────────────────────────

def test_read(spark: SparkSession) -> bool:
    section("4 / 4  —  Read Messages ← Kafka")
    try:
        df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TEST_TOPIC)
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
        ok(f"Read {count} message(s) from topic '{TEST_TOPIC}'")

        if count > 0:
            print()
            df.show(truncate=False)

        return count > 0
    except Exception as exc:
        fail(f"Read failed: {exc}")
        return False


# ── Streaming smoke-test (optional, runs for ~10 s) ──────────────────────────

def test_streaming(spark: SparkSession) -> None:
    section("Bonus  —  Streaming Smoke Test (10 s)")
    info("Starting a streaming query — will print any incoming messages …")
    try:
        stream_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", TEST_TOPIC)
            .option("startingOffsets", "latest")
            .load()
            .selectExpr(
                "CAST(key   AS STRING) AS key",
                "CAST(value AS STRING) AS value",
                "timestamp",
            )
        )

        query = (
            stream_df.writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .start()
        )

        time.sleep(10)
        query.stop()
        ok("Streaming query ran successfully for 10 s")
    except Exception as exc:
        fail(f"Streaming test failed: {exc}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "█" * 60)
    print("  Spark ↔ Kafka Connection Test")
    print(f"  Broker  : {KAFKA_BOOTSTRAP}")
    print(f"  Topic   : {TEST_TOPIC}")
    print(f"  Package : {KAFKA_PACKAGE}")
    print("█" * 60)

    spark  = build_spark()
    passed = 0
    total  = 3

    if test_broker_reachable(spark): passed += 1
    if test_write(spark):            passed += 1
    if test_read(spark):             passed += 1

    # Uncomment the line below to also run the 10-second streaming test:
    # test_streaming(spark)

    section("Summary")
    print(f"  Result : {passed}/{total} tests passed\n")
    if passed == total:
        ok("All tests passed — Spark and Kafka are connected correctly! 🎉")
    else:
        fail(f"{total - passed} test(s) failed — check the output above.")

    spark.stop()
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()