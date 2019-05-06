package spark.playground

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Normalizer, VectorAssembler}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor, LinearRegression, LinearRegressionModel}
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.immutable

object ModelTrainer_Gen extends LocalSparkContext {
  val useCsv: Boolean = true

  val outColumns: immutable.IndexedSeq[String] = (1 to 84).map { idx => s"outLink${idx}_vehOnRoad" }
  val inColumns: immutable.IndexedSeq[String] = (1 to 84).map { idx => s"inLink${idx}_vehOnRoad" }
  val allColumns = Array("vehOnRoad", "length") ++ outColumns ++ inColumns

  val LinkId = "51110"
  val seed: Long = 41L

  def main(args: Array[String]): Unit = {
    val mapping = spark.read.format("csv").option("header", "true").load("C:\\temp\\BeamRegression\\out_link_stat_mapping.csv").cache()
    val df = readCsv("C:\\temp\\BeamRegression\\out_link_stat.csv")
    df.show(10)

    // Split data into training (60%) and test (40%).
    val splits = df.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val labelColumn = "travelTime"
    val predictedColumn = "Predicted" + labelColumn
    val featureColumn = "features" // "normFeatures"

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
      .setMaxIter(50)

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(2.0)

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
    predictions.select(col("linkId"), col(labelColumn), col(predictedColumn)).show(100)

    //This will evaluate the error/deviation of the regression using the Root Mean Squared deviation
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("Predicted" + labelColumn)
      .setMetricName("rmse")

    //We compute the error using the evaluator
    val error = evaluator.evaluate(predictions)
    println(error)
    model.stages.collect {
      case lrModel: LinearRegressionModel =>
        println(s"LR Model coefficients:\n${lrModel.coefficients.toArray.mkString("\n")}")
        println(s"LR Model Intercept:\n${lrModel.intercept}")
      case gbtModel: GBTRegressionModel =>
        // println("Learned regression GBT model:\n" + gbtModel.toDebugString)
        val linkStatDf = mapping.filter(col("linkId") === LinkId)
        val featureImportance = getFeatureImportance(gbtModel, allColumns)
        val toSelect = featureImportance.flatMap { case (importance, name) =>
            val colName = name.split("_")(0)
            if (colName != "vehOnRoad")
              Array(colName + "_linkId", colName + "_numOfHops")
            else
              Array.empty[String]
        }
        linkStatDf.select(toSelect.head, toSelect.tail: _*).show()
        featureImportance.foreach { case (importance, name) =>
          println(s"$name: $importance")
        }
    }

  }

  private def readCsv(csvPath: String): DataFrame = {
    var df = spark.read.format("csv").option("header", "true").load(csvPath)
      .withColumn("travelTime", col("travelTime").cast(sql.types.IntegerType))
      .withColumn("vehOnRoad", col("vehOnRoad").cast(sql.types.IntegerType))
      .withColumn("length", col("length").cast(sql.types.DoubleType))
    outColumns.foreach { c =>
      df = df.withColumn(c, col(c).cast(sql.types.DoubleType))
    }
    inColumns.foreach { c =>
      df = df.withColumn(c, col(c).cast(sql.types.DoubleType))
    }
    df.na.fill(0).cache()
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
}
