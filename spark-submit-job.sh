#!/bin/bash
# Usage: ./spark-submit-job.sh your_job.py [extra spark-submit args]

if [ -z "$1" ]; then
  echo "Usage: ./spark-submit-job.sh <job.py> [extra args]"
  exit 1
fi

JOB=$1
shift  # remaining args passed straight to spark-submit

PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,\
org.apache.kafka:kafka-clients:3.4.1,\
org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.5,\
org.apache.commons:commons-pool2:2.11.1"

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.extraJavaOptions="-Divy.home=/tmp/.ivy2" \
  --packages "$PACKAGES" \
  "$@" \
  /opt/spark/jobs/"$JOB"