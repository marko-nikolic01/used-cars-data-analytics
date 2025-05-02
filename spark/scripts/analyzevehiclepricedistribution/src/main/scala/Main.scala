import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sys.env
import org.apache.spark.sql.expressions.Window

object Main {
  def main(args: Array[String]): Unit = {

    val MONGO_URI = env("MONGO_URI")
    val MONGO_DATABASE = "used_cars"
    val MONGO_COLLECTION = "price_distribution_by_year"
    val MONGO_CONNECTION_URI = s"$MONGO_URI/$MONGO_DATABASE"

    val HDFS_NAMENODE = env("CORE_CONF_fs_defaultFS")
    val TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/data/transformation/used_cars/"

    val spark = SparkSession.builder()
      .appName("Analyze Price Distribution by Year")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0")
      .config("spark.mongodb.connection.uri", MONGO_URI)
      .config("spark.mongodb.database", MONGO_DATABASE)
      .getOrCreate()

    import spark.implicits._

    // Load data from parquet files
    val df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

    // Find the max price in the dataset to determine the price range
    val maxPrice = df.agg(max($"price")).collect()(0).getDouble(0)

    // Create price ranges in increments of 1000
    // Create price ranges with "from" and "to"
    val priceRangeStartColumn = (floor($"price" / 1000) * 1000).alias("price_range_start")
    val priceRangeEndColumn = (floor($"price" / 1000) * 1000 + 999).alias("price_range_end")

    // Group data by year and price range, then count cars in each group
    val dfPriceDistribution = df
      .withColumn("price_range_start", priceRangeStartColumn)
      .withColumn("price_range_end", priceRangeEndColumn)
      .groupBy($"year", $"price_range_start", $"price_range_end")
      .agg(count("*").alias("count"))
      .orderBy($"year", $"price_range_start")

    // Write the result to MongoDB
    dfPriceDistribution.write
      .format("mongodb")
      .mode("overwrite")
      .option("spark.mongodb.connection.uri", MONGO_URI)
      .option("spark.mongodb.database", MONGO_DATABASE)
      .option("spark.mongodb.collection", MONGO_COLLECTION)
      .save()

    // Stop the Spark session
    spark.stop()
  }
}
