package spark.playground

import java.io.{BufferedReader, InputStreamReader}
import java.util.stream.Collectors

import com.microsoft.ml.spark.{LightGBMRegressionModel, LightGBMRegressor}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor, LinearRegressionModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

import scala.collection.immutable

object ModelTrainer_Gen extends LocalSparkContext {
  val Level: Int = 5
  val perLevelOutLinkFeatures: immutable.IndexedSeq[String] = (1 to Level).flatMap { lvl =>
    Array(s"L${lvl}_TotalVeh_OutLinks")
  }
  val perLevelInLinkFeatures: immutable.IndexedSeq[String] = (1 to Level).flatMap { lvl =>
    Array(s"L${lvl}_TotalVeh_InLinks")
  }

  // Array("vehOnRoad", "capacity", "lanes", "length", "FromNode_InLinksSize", "FromNode_OutLinksSize", "ToNode_InLinksSize", "ToNode_OutLinksSize")
  val allColumns: Array[String] = Array("vehicles_on_road") ++
    perLevelOutLinkFeatures ++ perLevelInLinkFeatures

  val featureColumns = allColumns.flatMap { col =>
    Seq("min_" + col,
        "max_" + col,
        "avg_" + col,
        "mean_" + col,
        "sum_" + col
    )
  }

  val seed: Long = 41L

  val linkIdsWithMoreThan1kDataPoints: Array[String] = getResourceFileAsString("link_ids").replaceAll("\r", "").split("\n")

  val trainGeneralizedModel: Boolean = true

  val numOfDatapointsPerLink: Int = 5000

  def showNulls(df: DataFrame): Unit = {
    val anyNull = df.columns.map(x => col(x).isNull).reduce(_ || _)
    df.filter(anyNull).show(100)
  }

  def makeStat(name: String): Seq[Column] = {
    val col: Column = new Column(name)
    Seq(min(col).as("min_" + col.toString()),
      max(col).as("max_" + col.toString()),
      avg(col).as("avg_" + col.toString()),
      mean(col).as("mean_" + col.toString()),
      sum(col).as("sum_" + col.toString())
    )
  }

  val dataReadyPath = """d:/Work/beam/TravelTimePrediction/sf-light/link_stats_5_aggregated_100_seconds_window"""
  val isDataReady: Boolean = false
  val shouldWriteJoinedData: Boolean = false

  def main(args: Array[String]): Unit = {
    val metaDataPath: String = args(0) // """D:\Work\beam\TravelTimePrediction\production-sfbay\Metadata_5.parquet"""
    val linkStatPath: String = args(1) // """D:\Work\beam\TravelTimePrediction\production-sfbay\link_stats_5.parquet""" // """D:\Work\beam\production-sfbay\link_stats_5.parquet"""
    val windowDuration: String = args(2)

    println(s"Level: $Level, trainGeneralizedModel: $trainGeneralizedModel, numOfDatapointsPerLink: $numOfDatapointsPerLink")
    println(s"windowDuration: $windowDuration, metaDataPath: $metaDataPath, linkStatPath: $linkStatPath")
    val s = System.currentTimeMillis()

//    spark.read.parquet(linkStatPath).
//      groupBy("link_id").agg(count("*").as("cnt"))
//      .orderBy(col("cnt").desc)
//      .persist(StorageLevel.MEMORY_ONLY)
//      .show(200000)

    val df = if (!isDataReady) {
//      val metaData = {
//        var md = spark.read.parquet(metaDataPath)
//        (1 to Level).foreach { lvl =>
//          md = md.withColumn(s"L${lvl}_flow_capacity_out_in_ratio",
//            col(s"L${lvl}_TotalFlowCapacity_OutLinks") / col(s"L${lvl}_TotalFlowCapacity_InLinks"))
//        }
//        md.na.fill(0.0).persist(StorageLevel.MEMORY_ONLY)
//      }

      println(col("col"))
      println(col("col").expr.nodeName)

      val linkStatDf = {
        val avgStats = allColumns.flatMap(makeStat)
        //.where(col("enter_time") >= 7*3600 && col("enter_time") <= 11*3600)
        val allNeededCol: Array[String] = allColumns ++ Array("leave_time", "travel_time", "ts")
        val df =
          spark.read.parquet(linkStatPath).withColumn("ts", col("leave_time").cast(TimestampType))
          .select("link_id",  allNeededCol :_*)
         .groupBy(col("link_id"), window(col("ts"), windowDuration))
         .agg(avg(col("travel_time")).as("travel_time"),
           avgStats: _*).persist(StorageLevel.MEMORY_ONLY)
        // df.coalesce(1).write.parquet(dataReadyPath)
        df.show(100, false)
        println(s"Written in ${System.currentTimeMillis() - s} ms")

        // throw new Exception("ASD")
//        val enoughDatapointsPerLink = df.groupBy("link_id").agg(count("*").as("cnt"))
//          .filter(col("cnt") >= numOfDatapointsPerLink)
//          .orderBy(col("cnt").desc)
//          .withColumn("row_id", row_number().over(Window.orderBy(col("cnt").desc)))
//          .filter(col("row_id") <= topN)
//          .persist(StorageLevel.MEMORY_ONLY)
//
//        enoughDatapointsPerLink.show(100)
//        enoughDatapointsPerLink.describe().show()
//
//        df.join(enoughDatapointsPerLink, Seq("link_id"), "inner")
//          .drop(col("cnt"))
//          .drop(col("row_id"))
        df
      }
      // linkStatDf.coalesce(1).write.parquet(dataReadyPath)
      linkStatDf
    }
    else {
      spark.read.parquet(dataReadyPath)
    }

    println(df.count())
    println(s"Dataframe size is: ${SizeEstimator.estimate(df)}")

    if (!trainGeneralizedModel) {
      linkIdsWithMoreThan1kDataPoints.foreach { linkId =>
        val linkDf = df.filter(col("link_id") === linkId)
        train(linkDf)
      }
    }
    else {
      train(df)
    }

    val e = System.currentTimeMillis()
    println(s"Executed in ${e - s} ms")
  }

  def train(df: DataFrame): Unit = {
    val splits = df.randomSplit(Array(0.8, 0.2), seed = seed)
    val training = splits(0).repartition(400)
    val test = splits(1)

    println(s"Training dataframe size is: ${SizeEstimator.estimate(training)}")

    val labelColumn = "travel_time"
    val predictedColumn = "Predicted" + labelColumn
    val featureColumn = "features"

    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("features")

    val gbt = new GBTRegressor()
      .setSeed(seed)
      .setLabelCol(labelColumn)
      .setFeaturesCol(featureColumn)
      .setPredictionCol(predictedColumn)
      .setMaxIter(10)

    val lgbm = new LightGBMRegressor()
      .setBaggingSeed(seed.toInt)
      .setLabelCol(labelColumn)
      .setFeaturesCol(featureColumn)
      .setPredictionCol(predictedColumn)

    val stages = Array(
      assembler,
      lgbm
    )

    val pipeline = new Pipeline().setStages(stages)
    //We fit our DataFrame into the pipeline to generate a model
    val model = pipeline.fit(training)
    //We'll make predictions using the model and the test data
    val predictions = model.transform(test)
    //This will evaluate the error/deviation of the regression using the Root Mean Squared deviation
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("Predicted" + labelColumn)
    println
    println
    println(s"rootMeanSquaredError: ${evaluator.setMetricName("rmse").evaluate(predictions)}")
    println(s"meanSquaredError: ${evaluator.setMetricName("mse").evaluate(predictions)}")
    println(s"meanAbsoluteError: ${evaluator.setMetricName("mae").evaluate(predictions)}")
    println(s"r2: ${evaluator.setMetricName("r2").evaluate(predictions)}")

    val predWithError = predictions
      .withColumn("absErr", abs(col(labelColumn) - col(predictedColumn)))
      .withColumn("percentErr", lit(100) * (col(labelColumn) - col(predictedColumn)) / col(labelColumn))
      .persist(StorageLevel.MEMORY_ONLY)

    predWithError.select(col("link_id"), col(labelColumn), col(predictedColumn),
      col("absErr"), col("percentErr")).show(50)

    predWithError.orderBy(col("absErr").desc)
      .select(col("link_id"), col(labelColumn), col(predictedColumn),
        col("absErr"), col("percentErr"), col("*")).show(10)
    predWithError.orderBy(col("absErr"))
      .select(col("link_id"), col(labelColumn), col(predictedColumn),
        col("absErr"), col("percentErr"), col("*")).show(10)
    predWithError.orderBy(col("percentErr"))
      .select(col("link_id"), col(labelColumn), col(predictedColumn),
        col("absErr"), col("percentErr"), col("*")).show(10)
    predWithError.orderBy(col("percentErr").desc)
      .select(col("link_id"), col(labelColumn), col(predictedColumn),
        col("absErr"), col("percentErr"), col("*")).show(10)

    model.stages.collect {
      case lrModel: LinearRegressionModel =>
        val coefficients = lrModel.coefficients.toArray.zipWithIndex.map { case (c, idx) => (c, allColumns(idx)) }
          .filter { case (c, _) => c != 0 }
        coefficients.foreach { case (c, name) =>
          println(s"$name: $c")
        }
        println(s"LR Model Intercept: ${lrModel.intercept}")
      case gbtModel: GBTRegressionModel =>
        // println("Learned regression GBT model:\n" + gbtModel.toDebugString)
        val featureImportance = getFeatureImportance(gbtModel, allColumns)
        featureImportance.foreach { case (importance, name) =>
          println(s"$name: $importance")
        }
      case lgbmModel: LightGBMRegressionModel =>
        // println("Learned regression GBT model:\n" + lgbmModel.toDebugString)
        println("gain")
        getFeatureImportance(lgbmModel, featureColumns, "gain").foreach { case (importance, name) =>
          println(s"$name: $importance")
        }
        println("split")
        getFeatureImportance(lgbmModel, featureColumns, "split").foreach { case (importance, name) =>
          println(s"$name: $importance")
        }
    }
  }

  private def getFeatureImportance(gbtModel: GBTRegressionModel, allColumns: Array[String]): Array[(Double, String)] = {
    val featureImportance = gbtModel.featureImportances.toArray.zipWithIndex.map { case (importance, idx) => (importance, allColumns(idx)) }
    featureImportance.filter { case (importance, _) => importance > 0 }.sortBy(x => -x._1)
  }

  private def getFeatureImportance(gbtModel: LightGBMRegressionModel, allColumns: Array[String], importanceType: String): Array[(Double, String)] = {
    val featureImportance = gbtModel.getFeatureImportances(importanceType).zipWithIndex.map { case (importance, idx) => (importance, allColumns(idx)) }
    featureImportance.filter { case (importance, _) => importance > 0 }.sortBy(x => -x._1)
  }

  def getResourceFileAsString(fileName: String): String = {
    val is = getClass.getClassLoader.getResourceAsStream(fileName)
    if (is != null) {
      val reader = new BufferedReader(new InputStreamReader(is))
      reader.lines.collect(Collectors.joining(System.lineSeparator))
    }
    else throw new Exception(s"Resource $fileName not found")

  }
}
