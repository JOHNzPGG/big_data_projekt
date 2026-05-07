import findspark
import time
from pyspark.sql import SparkSession

findspark.init("/opt/spark")

session = SparkSession.builder \
    .master("local[*]") \
    .appName("Christophorus Connection Check") \
    .getOrCreate()

print(f"Connected to Spark version: {session.version} ")
time.sleep(60)
print(f"Disconnected from Spark")
session.stop()