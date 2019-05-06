package spark.playground

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Normalizer, VectorAssembler}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor, LinearRegression}
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ModelTrainer_OnlyLinkStats extends LocalSparkContext {

  val useCsv: Boolean = true

  def main(args: Array[String]): Unit = {
    val actualTravelTimes = if (useCsv) readLinkStatsCsv("C:/temp/BeamRegression/0.linkstats.csv.gz")
    else {
      spark.read.parquet("C:/temp/BeamRegression/0.linkstats.parquet")
    }

    actualTravelTimes.show(100)

    // Split data into training (80%) and test (20%).
    val splits = actualTravelTimes.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val labelColumn = "traveltime"
    val featureColumn = "features"

    val columnName = Array("hour", "length", "freespeed", "capacity", "volume")
    val assembler = new VectorAssembler()
      .setInputCols(columnName)
      .setOutputCol("features")

    val lr = new LinearRegression()
      .setLabelCol(labelColumn)
      .setFeaturesCol(featureColumn)
      .setPredictionCol("Predicted" + labelColumn)
      .setMaxIter(100)

    val gbt = new GBTRegressor()
      .setLabelCol(labelColumn)
      .setFeaturesCol(featureColumn)
      .setPredictionCol("Predicted" + labelColumn)
      .setMaxIter(50)

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(2.0)

    val stages = Array(
      assembler,
      //      normalizer,
      lr
    )

    val pipeline = new Pipeline().setStages(stages)
    //We fit our DataFrame into the pipeline to generate a model
    val model = pipeline.fit(training)
    //We'll make predictions using the model and the test data
    val predictions = model.transform(test)
    predictions.show(100)

    //This will evaluate the error/deviation of the regression using the Root Mean Squared deviation
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("Predicted" + labelColumn)
      .setMetricName("rmse")

    //We compute the error using the evaluator
    val error = evaluator.evaluate(predictions)
    println(error)

    if (stages.exists(s => s.isInstanceOf[GBTRegressionModel])) {
      val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
      // println("Learned regression GBT model:\n" + gbtModel.toDebugString)
      println(gbtModel.featureImportances.toArray.zipWithIndex.map { case (importance, idx) => s"${columnName(idx)} = $importance" }.mkString(","))
    }
  }

  private def readLinkStatsCsv(csvPath: String): DataFrame = {
    spark.read.format("csv").option("header", "true").load(csvPath)
      .filter(col("stat") === "AVG" && col("hour") =!= "0.0 - 30.0")
      .withColumn("link", col("link").cast(sql.types.IntegerType))
      .withColumn("hour", col("hour").cast(sql.types.IntegerType))
      .withColumn("length", col("length").cast(sql.types.DoubleType))
      .withColumn("freespeed", col("freespeed").cast(sql.types.DoubleType))
      .withColumn("capacity", col("capacity").cast(sql.types.DoubleType))
      .withColumn("volume", col("volume").cast(sql.types.DoubleType))
      .withColumn("traveltime", col("traveltime").cast(sql.types.DoubleType))
      .cache()
  }
}
