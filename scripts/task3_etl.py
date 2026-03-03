from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum

spark = SparkSession.builder \
    .appName("Task3_Transformations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/adsremedy.daily_customer_totals") \
    .getOrCreate()

# 3.1 Data Extraction
df = spark.read.format("delta").load("/opt/bitnami/spark/data/delta-lake/customer_transactions")

# 3.2 Transformations
# Requirement: Deduplicate by transaction_id
# Requirement: Filter amount > 0
# Requirement: Add transaction_date column
transformed_df = df.dropDuplicates(["transaction_id"]) \
    .filter(col("amount") > 0) \
    .withColumn("transaction_date", to_date(col("timestamp")))

# 3.2 Aggregation: Daily totals for each customer
final_df = transformed_df.groupBy("customer_id", "transaction_date") \
    .agg(sum("amount").alias("daily_total"))

# Show results
final_df.show(10)

# Task 4.2: Load to NoSQL (MongoDB)
final_df.write.format("mongo").mode("append").save()
print("Task 3 and 4 successfully completed!")