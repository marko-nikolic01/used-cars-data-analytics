# /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 /home/scripts/analyze_vehicle_prices_by_model.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.window import Window

# MongoDB configuration
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "used_cars"
MONGO_COLLECTION = "price_analysis_by_model"
MONGO_CONNECTION_URI = f"{MONGO_URI}/{MONGO_DATABASE}"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Analyze vehicle price by model") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()

# Paths for HDFS and transformation zone
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/data/transformation/used_cars/"

# Read cleaned used cars data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

# Rename column year to make_year
df = df.withColumnRenamed("year", "make_year")

# Define a window
window = Window.partitionBy("make_name", "model_name", "make_year", "listed_date").orderBy("listed_date")

# Calculate average price by model for every day it was listed on the market
df_avg_price = df.withColumn("avg_price", avg("price").over(window))

# Remove duplicates
df_avg_price = df_avg_price.select("make_name", "model_name", "make_year", "listed_date", "avg_price").distinct()

# Save to MongoDB
df_avg_price.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

# Stop Spark Session
spark.stop()
