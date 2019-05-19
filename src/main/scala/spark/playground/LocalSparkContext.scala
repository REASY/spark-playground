package spark.playground

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

trait LocalSparkContext {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Local Spark example")
    .config("spark.master", "local")
    .master("local[1]")
    .config("spark.ui.port", "4080")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.memory", "24g")
    .config("spark.executor.instances", "1")
    .config("spark.executor.cores", "2")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("OFF")

  val sqlContext: SQLContext = spark.sqlContext
}