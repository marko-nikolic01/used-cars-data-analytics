# /spark/bin/spark-submit --master spark://spark-master:7077 /home/scripts/clean_used_cars_data.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql.types import DoubleType, IntegerType, BooleanType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Clean used cars data") \
    .getOrCreate()

# Paths for HDFS, raw and transformation zones
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
RAW_DATA_PATH = HDFS_NAMENODE + "/data/raw/used_cars/"
TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/data/transformation/used_cars/"

# Read raw used cars data
df_raw = spark.read.option("header", True).csv(RAW_DATA_PATH)

# Rename columns
columns_rename = {
    "daysonmarket": "days_on_market"
}
for old_name, new_name in columns_rename.items():
    df_raw = df_raw.withColumnRenamed(old_name, new_name)

# Remove unnecessary columns
columns_to_keep = [
    "vin", "body_type", "city", "city_fuel_economy", "combine_fuel_economy", 
    "days_on_market", "exterior_color","frame_damaged","fuel_type","has_accidents",
    "height","highway_fuel_economy","horsepower","latitude","length","listed_date",
    "longitude","make_name","mileage","model_name","owner_count","price",
    "transmission","width","year"
]
df_cleaned = df_raw.select(columns_to_keep)

# Remove rows with missing values
required_columns = [
    "vin", "listed_date", "make_name", "mileage", "model_name", "price", "year"
]
df_cleaned = df_cleaned.dropna(subset=required_columns)

# Convert height, length, and width from inches to centimeters
inToCm = 2.54
for column in ["height", "length", "width"]:
    df_cleaned = df_cleaned.withColumn(
        column,
        (regexp_replace(col(column), " in", "").cast(DoubleType()) * inToCm).alias(column)
    )

# Convert numeric data types
numeric_columns = {
    "city_fuel_economy": DoubleType(),
    "combine_fuel_economy": DoubleType(),
    "days_on_market": IntegerType(),
    "highway_fuel_economy": DoubleType(),
    "horsepower": DoubleType(),
    "latitude": DoubleType(),
    "longitude": DoubleType(),
    "mileage": DoubleType(),
    "owner_count": DoubleType(),
    "price": DoubleType(),
    "year": IntegerType()
}
for col_name, dtype in numeric_columns.items():
    df_cleaned = df_cleaned.withColumn(col_name, col(col_name).cast(dtype))

# Convert boolean columns
boolean_columns = {
    "frame_damaged",
    "has_accidents"
}
for col_name in boolean_columns:
    df_cleaned = df_cleaned.withColumn(
        col_name,
        when(col(col_name).isin("True", "true"), True)
        .when(col(col_name).isin("False", "false"), False)
        .otherwise(False)
        .cast(BooleanType())
    )

# Calculate combine_fuel_economy where it is empty
df_cleaned = df_cleaned.withColumn(
    "combine_fuel_economy",
    when(
        (col("combine_fuel_economy").isNull()) & 
        (col("city_fuel_economy").isNotNull()) & 
        (col("highway_fuel_economy").isNotNull()),
        (0.55 * col("city_fuel_economy")) + (0.45 * col("highway_fuel_economy"))
    ).otherwise(col("combine_fuel_economy"))
)

# Convert fuel economies from km/l to l/100km
fuel_columns = ["city_fuel_economy", "combine_fuel_economy", "highway_fuel_economy"]
for col_name in fuel_columns:
    df_cleaned = df_cleaned.withColumn(
        col_name, when(col(col_name).isNotNull(), 100 / col(col_name)).cast(DoubleType())
    )

# Save cleaned data in transformation zone
df_cleaned.write.mode("overwrite").parquet(TRANSFORMATION_DATA_PATH)

# Stop Spark Session
spark.stop()