#!/bin/bash
# CDC Consumer launcher script
#
# Required environment variables:
#   CONFIG_DIR - Path to config directory
#   CATALOG_NAME - Iceberg catalog name
#   WAREHOUSE_PATH - Iceberg warehouse path
#
# Usage: ./run_cdc.sh <source_name>

SOURCE_NAME=${1:-dev_siebel}

# Validate required environment variables
if [ -z "$CONFIG_DIR" ]; then
    echo "ERROR: CONFIG_DIR environment variable must be set"
    exit 1
fi

if [ -z "$CATALOG_NAME" ]; then
    echo "ERROR: CATALOG_NAME environment variable must be set"
    exit 1
fi

if [ -z "$WAREHOUSE_PATH" ]; then
    echo "ERROR: WAREHOUSE_PATH environment variable must be set"
    exit 1
fi

# Auto-detect project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Use SPARK_HOME if set, otherwise assume spark-submit is in PATH
SPARK_SUBMIT=${SPARK_HOME:+$SPARK_HOME/bin/}spark-submit

$SPARK_SUBMIT \
  --master local[*] \
  --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar,/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.7.jar,/opt/spark/jars/kafka-clients-3.4.1.jar,/opt/spark/jars/commons-pool2-2.11.1.jar,/opt/spark/jars/ojdbc8.jar \
  --conf spark.driver.memory=4g \
  --conf spark.executor.memory=4g \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  "$PROJECT_ROOT/jobs/cdc_kafka_to_iceberg.py" \
  --source "$SOURCE_NAME"
