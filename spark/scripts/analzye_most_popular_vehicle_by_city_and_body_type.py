# /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 /home/scripts/analzye_most_popular_vehicle_by_city_and_body_type.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number, year
from pyspark.sql.window import Window

# MongoDB configuration
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "used_cars"
MONGO_COLLECTION = "most_popular_vehicle_analysis_by_city_and_body_type"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Analyze most popular vehicles by city and body type") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()

# Paths for HDFS and transformation zone
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/data/transformation/used_cars/"

# Read cleaned used cars data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

# Remove rows with missing values for city or body type
df = df.na.drop(subset=["city", "body_type"])

# Rename column year to make_year
df = df.withColumnRenamed("year", "make_year")

# Extract listing year from the listed_date
df = df.withColumn("year", year(col("listed_date")))

# Define window to partition by city, body_type, and year, ordered by the count of vehicles
window = Window.partitionBy("city", "body_type", "year").orderBy(col("count").desc())

# Group by city, body_type, year, make_name, make_model and count occurrences
df_popular = df.groupBy("city", "body_type", "year", "make_name", "model_name") \
    .agg(count("*").alias("count"))

# Add row number to find the most popular vehicle
df_popular = df_popular.withColumn("row_num", row_number().over(window))

# Filter to get the most popular vehicles
df_popular = df_popular.filter(col("row_num") == 1).drop("row_num")

# Save the result to MongoDB
df_popular.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

# Stop Spark Session
spark.stop()
