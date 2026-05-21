#!/bin/bash
if [ -z "$1" ]; then
  echo "Usage: ./spark-submit-job.sh <job.py> [extra args]"
  exit 1
fi

JOB=$1
shift

JOB_DIR="/home/iceberg/notebooks/notebooks"

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,\
org.apache.kafka:kafka-clients:3.4.1,\
org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.5,\
org.apache.commons:commons-pool2:2.11.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.367"

docker exec spark-iceberg /opt/spark/bin/spark-submit \
  --master local[*] \
  --executor-memory 1g \
  --executor-cores 2 \
  --total-executor-cores 2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.extraJavaOptions="-Divy.home=/tmp/.ivy2" \
  --packages "$PACKAGES" \
  "$@" \
  "$JOB_DIR/$JOB"