import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import sys.env

object Main {
  def main(args: Array[String]): Unit = {

    // MongoDB configuration
    val MONGO_URI = env("MONGO_URI")
    val MONGO_DATABASE = "used_cars"
    val MONGO_COLLECTION = "vehicle_color_impact_on_price_and_days_on_market"
    val MONGO_CONNECTION_URI = s"$MONGO_URI/$MONGO_DATABASE"

    val spark = SparkSession.builder()
      .appName("Analyze vehicle color impact on price and days on market")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0")
      .config("spark.mongodb.connection.uri", MONGO_URI)
      .config("spark.mongodb.database", MONGO_DATABASE)
      .getOrCreate()

    import spark.implicits._

    // Paths for HDFS and transformation zone
    val HDFS_NAMENODE = env("CORE_CONF_fs_defaultFS")
    val TRANSFORMATION_DATA_PATH = HDFS_NAMENODE + "/data/transformation/used_cars/"

    // Read cleaned used cars data
    val df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

    // Select only needed columns and filter out missing values
    val dfFiltered = df
      .select("make_name", "model_name", "year", "exterior_color", "price", "days_on_market")
      .na.drop(Seq("exterior_color", "days_on_market"))

    // Calculate average price and average days on market grouped by model and exterior color
    val dfAverage = dfFiltered
      .groupBy("make_name", "model_name", "year", "exterior_color")
      .agg(
        avg("price").alias("avg_price_by_color"),
        avg("days_on_market").alias("avg_dom_by_color")
      )

    // Calculate average price and average days on market grouped by model
    val dfAverageByModel = dfFiltered
      .groupBy("make_name", "model_name", "year")
      .agg(
        avg("price").alias("avg_price_all_colors"),
        avg("days_on_market").alias("avg_dom_all_colors")
      )

    // Join and calculate differences and percent differences
    val dfResult = dfAverage
      .join(dfAverageByModel, Seq("make_name", "model_name", "year"))
      .withColumn("price_diff", round($"avg_price_by_color" - $"avg_price_all_colors", 2))
      .withColumn("dom_diff", round($"avg_dom_by_color" - $"avg_dom_all_colors", 2))
      .withColumn("price_diff_with_sign", 
        when($"price_diff" > 0, concat(lit("$USD +"), $"price_diff"))
        .otherwise(concat(lit("$USD "), $"price_diff"))
      )
      .withColumn("dom_diff_with_sign", 
        when($"dom_diff" > 0, concat(lit("+"), $"dom_diff"))
        .otherwise($"dom_diff")
      )
      .withColumn("price_percent_diff", 
        round((($"avg_price_by_color" - $"avg_price_all_colors") / $"avg_price_all_colors") * 100, 2)
      )
      .withColumn("dom_percent_diff", 
        round((($"avg_dom_by_color" - $"avg_dom_all_colors") / $"avg_dom_all_colors") * 100, 2)
      )
      .withColumn("price_percent_diff_with_sign", 
        when($"price_percent_diff" > 0, concat(lit("+"), $"price_percent_diff".cast("string"), lit(" %")))
        .otherwise(concat($"price_percent_diff".cast("string"), lit(" %")))
      )
      .withColumn("dom_percent_diff_with_sign", 
        when($"dom_percent_diff" > 0, concat(lit("+"), $"dom_percent_diff".cast("string"), lit(" %")))
        .otherwise(concat($"dom_percent_diff".cast("string"), lit(" %")))
      )

    // Write result to MongoDB
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
