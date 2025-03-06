# /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 /home/scripts/analyze_vehicle_offer_by_fuel_type_and_month.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, month, year, sum, concat_ws, lpad

# MongoDB configuration
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "used_cars"
MONGO_COLLECTION = "fuel_type_analysis_by_month"
MONGO_CONNECTION_URI = f"{MONGO_URI}/{MONGO_DATABASE}"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Analyze vehicle offers by fuel type and month") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()


# Paths for HDFS and transformation zone
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/data/transformation/used_cars/"

# Read cleaned used cars data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)
df = df.filter(col("fuel_type").isNotNull())

# Extract month and year
df = df.withColumn("month", month(col("listed_date"))).withColumn("year", year(col("listed_date")))
df = df.filter(col("listed_date") >= "2013-09-01")

# Get unique months and years
df_dates = df.select("year", "month").distinct()

# Get unique fuel types
df_fuel = df.select("fuel_type").distinct()

# Create all possible (year, month, fuel_type) combinations
df_date_fuel_combinations = df_dates.crossJoin(df_fuel)

# Count number of cars per fuel type per month
df_fuel_count = df.groupBy("year", "month", "fuel_type").agg(count("*").alias("count"))

# Ensure every (year, month, fuel_type) is represented, filling missing counts with 0
df_fuel_count = df_date_fuel_combinations.join(df_fuel_count, ["year", "month", "fuel_type"], "left_outer") \
    .fillna(0, subset=["count"])

# Total number of cars listed per month
df_total_count = df.groupBy("year", "month").agg(count("*").alias("total_count"))

# Join to calculate percentages
df_fuel_percentage = df_fuel_count.join(df_total_count, ["year", "month"], "inner") \
    .withColumn("percentage", round((col("count") / col("total_count")) * 100, 5))

# Pivot to display percentages of fuel types by year/month
df_pivot = df_fuel_percentage.groupBy("year", "month") \
    .pivot("fuel_type") \
    .agg(sum("percentage"))

# Sort by month
df_sorted = df_pivot.orderBy("year", "month")
df_sorted = df_sorted.withColumn("year_month", concat_ws("-", col("year"), lpad(col("month"), 2, "0"))).drop("year", "month")

# Save to database
df_sorted.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

# Stop Spark Session
spark.stop()
