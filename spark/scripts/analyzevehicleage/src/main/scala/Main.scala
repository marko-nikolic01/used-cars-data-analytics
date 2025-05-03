import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sys.env

object Main {
  def main(args: Array[String]): Unit = {

    // MongoDB configuration
    val MONGO_URI = env("MONGO_URI")
    val MONGO_DATABASE = "used_cars"
    val MONGO_COLLECTION = "vehicle_age_analysis"
    val MONGO_CONNECTION_URI = s"$MONGO_URI/$MONGO_DATABASE"

    val spark = SparkSession.builder()
      .appName("Analyze vehicle age")
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

    // Extract listing year from listed_date
    val dfWithListingYear = df.withColumn("listing_year", year($"listed_date"))

    // Group by listing year and make year, and count vehicles
    val dfAgeDistribution = dfWithListingYear
      .groupBy("listing_year", "year") // year = make year
      .agg(count("*").alias("count"))

    // Calculate average make year per listing year
    val dfAverageMakeYear = dfWithListingYear
      .groupBy("listing_year")
      .agg(round(avg($"year"), 2).alias("avg_make_year"))

    // Join counts with average make year
    val dfResult = dfAgeDistribution
      .join(dfAverageMakeYear, Seq("listing_year"))
      .orderBy("listing_year", "year")

    // Save to MongoDB
    dfResult.write
      .format("mongodb")
      .mode("overwrite")
      .option("spark.mongodb.connection.uri", MONGO_URI)
      .option("spark.mongodb.database", MONGO_DATABASE)
      .option("spark.mongodb.collection", MONGO_COLLECTION)
      .save()

    spark.stop()
  }
}
