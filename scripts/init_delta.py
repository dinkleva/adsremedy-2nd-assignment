from pyspark.sql import SparkSession
from delta.tables import *
import os

# Initialize SparkSession 

spark = SparkSession.builder \
    .appName("InitDeltaLake") \
    .getOrCreate()

# Define paths
json_path = "/opt/bitnami/spark/data/raw_transactions.json"
delta_path = "/opt/bitnami/spark/data/delta-lake/customer_transactions"

try:
    print(f"Reading data from: {json_path}")
    df = spark.read.json(json_path)

    print(f"Writing Delta table to: {delta_path}")
    df.write.format("delta").mode("overwrite").save(delta_path)

    print("Verifying Delta History...")
    dt = DeltaTable.forPath(spark, delta_path)
    dt.history().show()
    
    print("Delta Lake initialization successful!")

except Exception as e:
    print(f"Error during Delta initialization: {e}")
    raise e