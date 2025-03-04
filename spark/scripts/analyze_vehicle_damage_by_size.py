# /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 /home/scripts/analyze_vehicle_damage_by_size.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, when, lit

# MongoDB configuration
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "used_cars"
MONGO_COLLECTION = "damage_analysis_by_size"
MONGO_CONNECTION_URI = f"{MONGO_URI}/{MONGO_DATABASE}"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Analyze vehicle damage by size") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()

# Paths for HDFS and transformation zone
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/data/transformation/used_cars/"

# Read cleaned used cars data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

# Filter out records with missing length or width
df = df.filter((col("length").isNotNull()) & (col("width").isNotNull()))

# Calculate vehicle surface area
df = df.withColumn("surface_area", col("length") * col("width") / 10000.)

# Define size categories based on surface area
df = df.withColumn("size_category",
    when(col("surface_area") < 8, "0-8 m2")
    .when(col("surface_area") < 9, "8-9 m2")
    .when(col("surface_area") < 10, "9-10 m2")
    .when(col("surface_area") < 11, "10-11 m2")
    .when(col("surface_area") < 12, "11-12 m2")
    .otherwise("12+ m2")
)

# Count total vehicles per size category
df_total = df.groupBy("size_category").agg(count("*").alias("total_count"))

# Count damaged vehicles per size category
df_damaged = df.filter((col("frame_damaged") == True) | (col("has_accidents") == True)) \
    .groupBy("size_category") \
    .agg(count("*").alias("damaged_count"))

# Join to compute damage percentages
df_analysis = df_total.join(df_damaged, ["size_category"], "left_outer") \
    .fillna(0, subset=["damaged_count"]) \
    .withColumn("damaged_percentage", round((col("damaged_count") / col("total_count")) * 100, 2))

# Save to MongoDB
df_analysis.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

# Stop Spark Session
spark.stop()

