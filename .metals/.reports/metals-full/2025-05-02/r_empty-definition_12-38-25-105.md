error id: `<none>`.
file://<WORKSPACE>/spark/scripts/analyzevehiclepricedistribution/src/main/scala/Main.scala
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/functions.
	 -org/apache/spark/sql/functions#
	 -org/apache/spark/sql/functions().
	 -spark/implicits.
	 -spark/implicits#
	 -spark/implicits().
	 -.
	 -#
	 -().
	 -scala/Predef.
	 -scala/Predef#
	 -scala/Predef().
offset: 1070
uri: file://<WORKSPACE>/spark/scripts/analyzevehiclepricedistribution/src/main/scala/Main.scala
text:
```scala
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

    val df = spark.read.parquet(TRANSFORMATION_DATA_PATH)

    val windowSpec = Window.partitionBy($"year").orderBy(desc("count"))

    val dfPriceDistribution = df
      .filter($"year".i@@sNotNull && $"price".isNotNull)
      .groupBy($"year", $"price")
      .agg(count("*").alias("count"))

    dfPriceDistribution.write
      .format("mongodb")
      .mode("overwrite")
      .option("spark.mongodb.connection.uri", MONGO_URI)
      .option("spark.mongodb.database", MONGO_DATABASE)
      .option("spark.mongodb.collection", MONGO_COLLECTION)
      .save()

    spark.stop()
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.