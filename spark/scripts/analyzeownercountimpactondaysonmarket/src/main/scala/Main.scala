import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sys.env

object Main {
  def main(args: Array[String]): Unit = {

    // MongoDB configuration
    val MONGO_URI = env("MONGO_URI")
    val MONGO_DATABASE = "used_cars"
    val MONGO_COLLECTION = "days_on_market_by_owner_count"
    val MONGO_CONNECTION_URI = s"$MONGO_URI/$MONGO_DATABASE"

    val spark = SparkSession.builder()
      .appName("Analyze owner count impact on days on market")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0")
      .config("spark.mongodb.connection.uri", MONGO_URI)
      .config("spark.mongodb.database", MONGO_DATABASE)
      .getOrCreate()

    import spark.implicits._

    // Paths for HDFS and transformation zone
    val HDFS_NAMENODE = env("CORE_CONF_fs_defaultFS")
    val TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/data/transformation/used_cars/"

    // Read cleaned used cars data
    val df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

    // Filter out records with missing owner_count or days_on_market
    val filteredDf = df
      .filter($"owner_count".isNotNull && $"days_on_market".isNotNull)

    // Group by owner_count and calculate average, min, max, and count of listings
    val dfAnalysis = filteredDf
      .groupBy($"owner_count")
      .agg(
        avg($"days_on_market").alias("avg_days_on_market"),
        min($"days_on_market").alias("min_days_on_market"),
        max($"days_on_market").alias("max_days_on_market"),
        count("*").alias("listing_count")
      )
      .orderBy($"owner_count")

    // Write to MongoDB
    dfAnalysis.write
      .format("mongodb")
      .mode("overwrite")
      .option("spark.mongodb.connection.uri", MONGO_URI)
      .option("spark.mongodb.database", MONGO_DATABASE)
      .option("spark.mongodb.collection", MONGO_COLLECTION)
      .save()

    spark.stop()
  }
}
