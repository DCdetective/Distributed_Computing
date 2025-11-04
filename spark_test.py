import os
import sys

# ---- CRITICAL ENV SETUP (before importing PySpark) ----
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

os.environ['JAVA_OPTS'] = '--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED'

print("🔧 Environment setup complete")

from pyspark.sql import SparkSession
from pyspark import SparkConf

try:
    conf = SparkConf() \
        .setAppName("IoT Batch Analysis") \
        .setMaster("local[*]") \
        .set("spark.driver.host", "127.0.0.1") \
        .set("spark.driver.bindAddress", "127.0.0.1") \
        .set("spark.local.ip", "127.0.0.1") \
        .set("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .set("spark.python.worker.faulthandler.enabled", "true") \
        .set("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .set("spark.driver.extraJavaOptions",
             "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED")

    print("🚀 Creating Spark session...")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    print("✅ Spark session created successfully!")
    print(f"Spark version: {spark.version}")

    # ---- READ FROM SQLITE (IMPORTANT) ----
    db_path = "iot_data.db"

    df = spark.read.format("jdbc").options(
        url=f"jdbc:sqlite:{db_path}",
        dbtable="sensor_readings",
        driver="org.sqlite.JDBC"
    ).load()

    print("📌 Data loaded from SQLite")
    df.show(10)

    # ---- Basic Analytics ----
    print("📊 Average temperature & humidity per sensor:")
    df.groupBy("sensor_id").avg("temperature", "humidity").show()

    print("📈 Total records:")
    print(df.count())

    # ---- Show hottest sensor ----
    print("🔥 Highest temperature recorded:")
    df.orderBy(df.temperature.desc()).show(1)

    print("🎉 ANALYSIS COMPLETED!")

    spark.stop()

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
