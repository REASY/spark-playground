package spark.playground

import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.stream.Collectors

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Normalizer, StandardScaler, VectorAssembler}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor, LinearRegression, LinearRegressionModel}
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ModelTrainer_Gen extends LocalSparkContext {
  val Level: Int = 3
  val perLevelOutLinkFeatures = (1 to Level).flatMap { lvl =>
    Array(s"L${lvl}_TotalVeh_OutLinks",	s"L${lvl}_MinVeh_OutLinks", s"L${lvl}_MaxVeh_OutLinks", s"L${lvl}_MedianVeh_OutLinks", s"L${lvl}_AvgVeh_OutLinks", s"L${lvl}_StdVeh_OutLinks")
  }
  val perLevelInLinkFeatures = (1 to Level).flatMap { lvl =>
    Array(s"L${lvl}_TotalVeh_InLinks",	s"L${lvl}_MinVeh_InLinks", s"L${lvl}_MaxVeh_InLinks", s"L${lvl}_MedianVeh_InLinks", s"L${lvl}_AvgVeh_InLinks", s"L${lvl}_StdVeh_InLinks")
  }

  val allColumns: Array[String] = Array("vehOnRoad", "capacity", "lanes", "length", "FromNode_InLinksSize", "FromNode_OutLinksSize", "ToNode_InLinksSize", "ToNode_OutLinksSize") ++
    perLevelOutLinkFeatures ++ perLevelInLinkFeatures

  val seed: Long = 41L
  val someLinkIds: Array[String] = Array(
    "32000", "70506", "70084", "13400", "57838", "55972", "22669", "72854", "31386", "27883", "55946")

  val linkIdsWithMoreThan1kDataPoints: Array[String] = getResourceFileAsString("link_ids").replaceAll("\r", "").split("\n")

  val trainGeneralizedModel: Boolean = true

  def main(args: Array[String]): Unit = {
    println(s"trainGeneralizedModel: ${trainGeneralizedModel}")

    val metaData = spark.read.format("csv").option("header", "true").load("C:\\temp\\BeamRegression\\Metadata.csv").cache()
    val df = readCsv("C:\\temp\\BeamRegression\\link_stat.csv")
      .filter(col("linkId").isin(someLinkIds: _*))
      .join(metaData, Seq("linkId"), "inner")
      .withColumn("capacity", col("capacity").cast(sql.types.DoubleType))
      .withColumn("lanes", col("lanes").cast(sql.types.DoubleType))
      .withColumn("length", col("length").cast(sql.types.DoubleType))
      .withColumn("FromNode_InLinksSize", col("FromNode_InLinksSize").cast(sql.types.DoubleType))
      .withColumn("FromNode_OutLinksSize", col("FromNode_OutLinksSize").cast(sql.types.DoubleType))
      .withColumn("ToNode_InLinksSize", col("ToNode_InLinksSize").cast(sql.types.DoubleType))
      .withColumn("ToNode_OutLinksSize", col("ToNode_OutLinksSize").cast(sql.types.DoubleType))
      .cache()

    if (!trainGeneralizedModel) {
      df.show(10)
      someLinkIds.foreach { linkId =>
        val linkDf = df.filter(col("linkId") === linkId)
        train(linkDf)
      }
    }
    else {
      val genDf = df.filter(col("linkId").isin(linkIdsWithMoreThan1kDataPoints: _*))
      genDf.show(10)
      train(genDf)
    }
  }

  def train(df: DataFrame): Unit = {
    val splits = df.randomSplit(Array(0.8, 0.2), seed = seed)
    val training = splits(0).cache()
    val test = splits(1)

    val labelColumn = "travelTime"
    val predictedColumn = "Predicted" + labelColumn
    val featureColumn = "features"

    val assembler = new VectorAssembler()
      .setInputCols(allColumns)
      .setOutputCol("features")

    val lr = new LinearRegression()
      .setLabelCol(labelColumn)
      .setFeaturesCol(featureColumn)
      .setPredictionCol(predictedColumn)
      .setMaxIter(100)

    val gbt = new GBTRegressor()
      .setSeed(seed)
      .setLabelCol(labelColumn)
      .setFeaturesCol(featureColumn)
      .setPredictionCol(predictedColumn)
      .setMaxIter(10)

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(2.0)

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    val stages = Array(
      assembler,
      //      normalizer,
      gbt
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
      .withColumn("absErr", abs(col("travelTime") - col("PredictedtravelTime")))
      .withColumn("percentErr", lit(100) * (col("travelTime") - col("PredictedtravelTime")) / col("travelTime"))

    predWithError.select(col("linkId"), col(labelColumn), col(predictedColumn),
      col("absErr"), col("percentErr")).show(100)

    predWithError.orderBy(col("absErr").desc)
      .select(col("linkId"), col(labelColumn), col(predictedColumn),
        col("absErr"), col("percentErr"), col("*")).show(30)
    predWithError.orderBy(col("absErr"))
      .select(col("linkId"), col(labelColumn), col(predictedColumn),
        col("absErr"), col("percentErr"), col("*")).show(30)
    predWithError.orderBy(col("percentErr"))
      .select(col("linkId"), col(labelColumn), col(predictedColumn),
        col("absErr"), col("percentErr"), col("*")).show(30)
    predWithError.orderBy(col("percentErr").desc)
      .select(col("linkId"), col(labelColumn), col(predictedColumn),
        col("absErr"), col("percentErr"), col("*")).show(30)

    model.stages.collect {
      case lrModel: LinearRegressionModel =>
        val coefficients = lrModel.coefficients.toArray.zipWithIndex.map { case (c, idx) => (c, allColumns(idx)) }
          .filter { case (c, name) => c != 0 }
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

      //        val linkStatDf = mapping.filter(col("linkId") === LinkId)
      //
      //        val toSelect = featureImportance.flatMap { case (importance, name) =>
      //            val colName = name.split("_")(0)
      //            if (colName != "vehOnRoad")
      //              Array(colName + "_linkId", colName + "_numOfHops")
      //            else
      //              Array.empty[String]
      //        }
      //        linkStatDf.select(toSelect.head, toSelect.tail: _*).show()
      //        featureImportance.foreach { case (importance, name) =>
      //          println(s"$name: $importance")
      //        }
    }
  }

  private def readCsv(csvPath: String): DataFrame = {
    var df = spark.read.format("csv").option("header", "true").load(csvPath)
      .withColumn("lid", col("linkId").cast(sql.types.IntegerType))
      .withColumn("travelTime", col("travelTime").cast(sql.types.DoubleType))
      .withColumn("vehOnRoad", col("vehOnRoad").cast(sql.types.DoubleType))
    perLevelOutLinkFeatures.foreach { c =>
      df = df.withColumn(c, col(c).cast(sql.types.DoubleType))
    }
    perLevelInLinkFeatures.foreach { c =>
      df = df.withColumn(c, col(c).cast(sql.types.DoubleType))
    }
    df.na.fill(0)
  }

  private def getFeatureImportance(gbtModel: GBTRegressionModel, allColumns: Array[String]): Array[(Double, String)] = {
    val featureImportance = gbtModel.featureImportances.toArray.zipWithIndex.map { case (importance, idx) => (importance, allColumns(idx)) }
    featureImportance.filter { case (importance, name) => importance > 0 }.sortBy(x => -x._1)
  }

  private def printFeatureImportance(gbtModel: GBTRegressionModel, allColumns: Array[String]): Unit = {
    getFeatureImportance(gbtModel, allColumns).foreach { case (name, importance) =>
      println(s"$name: $importance")
    }
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
