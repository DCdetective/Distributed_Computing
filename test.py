import os
import sys

# ================= ENV FIXES (CRITICAL on Windows) =================

# Make Spark bind to a real IP
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Use the current virtualenv python for workers & driver
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Java 17 module access errors fix
os.environ['JAVA_OPTS'] = '--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED'

print(" Environment setup complete")

# ===================================================================

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

try:
    conf = SparkConf() \
        .setAppName("KafkaStream") \
        .setMaster("local[*]") \
        .set("spark.driver.host", "127.0.0.1") \
        .set("spark.driver.bindAddress", "127.0.0.1") \
        .set("spark.local.ip", "127.0.0.1") \
        .set("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .set("spark.driver.extraJavaOptions", 
             "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED")

    print(" Creating Spark session...")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    print(" Spark session created!")
    print(f"Spark version: {spark.version}")

    # ===== Schema for JSON payload ===== #
    schema = StructType([
        StructField("sensor_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType())
    ])

    print("📡 Connecting to Kafka...")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "iot-sensors") \
        .load()

    # Convert Kafka VALUE (binary) → JSON → columns
    json_df = df.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), schema).alias("data")) \
                .select("data.*")

    # Output to console
    query = json_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    print(" Streaming started. Waiting for messages...")
    query.awaitTermination()

except Exception as e:
    print(f" Error: {e}")
    import traceback
    traceback.print_exc()
