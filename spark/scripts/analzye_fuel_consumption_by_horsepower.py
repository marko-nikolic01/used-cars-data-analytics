# /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 /home/scripts/analzye_fuel_consumption_by_horsepower.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.window import Window

# MongoDB configuration
MONGO_URI = os.environ['MONGO_URI']
MONGO_DATABASE = "used_cars"
MONGO_COLLECTION = "fuel_consumption_analysis_by_horsepower"
MONGO_CONNECTION_URI = f"{MONGO_URI}/{MONGO_DATABASE}"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Analyze fuel consumption by horsepower") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.mongodb.database", MONGO_DATABASE) \
    .getOrCreate()

# Paths for HDFS and transformation zone
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/data/transformation/used_cars/"

# Read cleaned used cars data
df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

# Filter only gasoline cars
df = df.filter(df["fuel_type"] == "Gasoline")

# Remove rows where horsepower, city, combine or highway fuel consumption is missing
df = df.na.drop(subset=["horsepower", "city_fuel_economy", "combine_fuel_economy", "highway_fuel_economy"])

# Rename column year to make_year
df = df.withColumnRenamed("year", "make_year")

# Define window by make_year and horsepower
window = Window.partitionBy("make_year", "horsepower").orderBy("make_year", "horsepower")

# Compute moving averages for fuel economy
df_fuel_trends = df.withColumn("avg_city_fuel", avg("city_fuel_economy").over(window)) \
                            .withColumn("avg_combine_fuel", avg("combine_fuel_economy").over(window)) \
                            .withColumn("avg_highway_fuel", avg("highway_fuel_economy").over(window))

# Select final columns
df_fuel_trends = df_fuel_trends.select("make_year", "horsepower", "avg_city_fuel", "avg_combine_fuel", "avg_highway_fuel").distinct()

# Save to MongoDB
df_fuel_trends.write \
    .format("mongodb") \
    .mode("overwrite") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("spark.mongodb.database", MONGO_DATABASE) \
    .option("spark.mongodb.collection", MONGO_COLLECTION) \
    .save()

# Stop Spark Session
spark.stop()
