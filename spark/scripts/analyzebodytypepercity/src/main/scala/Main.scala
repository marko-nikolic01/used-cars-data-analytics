import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sys.env

object Main {
  def main(args: Array[String]): Unit = {

    // MongoDB configuration
    val MONGO_URI = env("MONGO_URI")
    val MONGO_DATABASE = "used_cars"
    val MONGO_COLLECTION = "body_type_count_by_city_year"
    val MONGO_CONNECTION_URI = s"$MONGO_URI/$MONGO_DATABASE"

    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Count body type by city and year")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0")
      .config("spark.mongodb.connection.uri", MONGO_URI)
      .config("spark.mongodb.database", MONGO_DATABASE)
      .getOrCreate()

    import spark.implicits._

    // Paths for HDFS and transformation zone
    val HDFS_NAMENODE = env("CORE_CONF_fs_defaultFS")
    val TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/data/transformation/used_cars/"

    // Read cleaned used cars data
    val validBodyTypes = Seq("Sedan", "SUV / Crossover", "Convertible", "Hatchback", "Coupe", "Van")
    val df = spark.read.parquet(TRANSFORMATION_DATA_PATH)
      .filter($"body_type".isin(validBodyTypes: _*))
      .filter($"city".isNotNull && $"body_type".isNotNull && $"listed_date".isNotNull)
      .withColumn("year", year($"listed_date"))

    // Count listings per city, year and body_type
    val dfCounts = df.groupBy("city", "year", "body_type")
      .agg(count("*").alias("count"))

    // Pivot to display body types as columns
    val dfPivot = dfCounts
      .groupBy("city", "year")
      .pivot("body_type", validBodyTypes)
      .agg(first("count"))
      .na.fill(0)  // Fill nulls with 0 if a body type doesn't exist for a city-year

    // Save to MongoDB
    dfPivot.write
      .format("mongodb")
      .mode("overwrite")
      .option("spark.mongodb.connection.uri", MONGO_URI)
      .option("spark.mongodb.database", MONGO_DATABASE)
      .option("spark.mongodb.collection", MONGO_COLLECTION)
      .save()

    spark.stop()
  }
}
