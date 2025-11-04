import os
import sys

# CRITICAL: Set these BEFORE importing PySpark
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'  # Use 127.0.0.1 instead of localhost
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Java 17 compatibility
os.environ['JAVA_OPTS'] = '--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED'

print("🔧 Environment setup complete")

from pyspark.sql import SparkSession
from pyspark import SparkConf

try:
    # Configure Spark with ALL necessary settings
    conf = SparkConf() \
        .setAppName("FinalTest") \
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
    
    # Simple test
    print("Testing basic operations...")
    df = spark.range(5).toDF("numbers")
    df.show()
    
    print("🎉 PYSPARK IS WORKING PERFECTLY!")
    
    spark.stop()
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()