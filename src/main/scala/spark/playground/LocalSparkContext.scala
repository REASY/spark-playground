package spark.playground

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

trait LocalSparkContext {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Local Spark example")
    .config("spark.master", "local")
    .master("local[*]")
    .config("spark.ui.port", "8000")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.memory", "6g")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("OFF")

  val sqlContext: SQLContext = spark.sqlContext
}