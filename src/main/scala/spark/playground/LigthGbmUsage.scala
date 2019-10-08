package spark.playground

import java.io.{BufferedReader, InputStreamReader}
import java.util.stream.Collectors

import com.microsoft.ml.spark.core.metrics.MetricConstants
import com.microsoft.ml.spark.core.schema.SchemaConstants
import com.microsoft.ml.spark.lightgbm.{LightGBMClassificationModel, LightGBMClassifier}
import com.microsoft.ml.spark.train.ComputeModelStatistics
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

object LigthGbmUsage extends LocalSparkContext {
  def main(args: Array[String]): Unit = {
    val df = spark.sqlContext.read.parquet("C:/Users/User/Downloads/part-00000-711560fe-8fdd-4777-a379-b52996fd212d-c000.gz.parquet")
    df.show()

    val featureColumns = Array[String]()

    val seed: Long = 41L
    val labelColumn = "label"
    val predictedColumn = "Predicted" + labelColumn
    val featureColumn = "scaledFeatures"


    val lgbm = new LightGBMClassifier()
      .setBaggingSeed(seed.toInt)
      .setLabelCol(labelColumn)
      .setFeaturesCol(featureColumn)
      .setPredictionCol(predictedColumn)

    val splits = df.randomSplit(Array(0.8, 0.2), seed = seed)
    val training = splits(0).cache()
    training.groupBy("label").count.show
    val test = splits(1).cache()
    test.groupBy("label").count.show

    val stages = Array(lgbm)
    val pipeline = new Pipeline().setStages(stages)

    //We fit our DataFrame into the pipeline to generate a model
    val model = pipeline.fit(training)
    //We'll make predictions using the model and the test data
    val predictions = model.transform(test)
    //This will evaluate the error/deviation of the regression using the Root Mean Squared deviation
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol(labelColumn)
      .setMetricName("areaUnderROC")

    println(evaluator.evaluate(predictions))

    val evaluatedData = new ComputeModelStatistics()
      .setLabelCol("label")
      .setScoredLabelsCol(predictedColumn)
      .setEvaluationMetric(MetricConstants.ClassificationMetricsName)
      .transform(predictions)

    evaluatedData.show(false)

    model.stages.collect {
      case lgbmModel: LightGBMClassificationModel =>
        // println("Learned regression GBT model:\n" + lgbmModel.toDebugString)
        println("gain")
        MetricHelper.getFeatureImportance(lgbmModel, featureColumns, "gain").foreach { case (importance, name) =>
          println(s"$name: $importance")
        }
        println("split")
        MetricHelper.getFeatureImportance(lgbmModel, featureColumns, "split").foreach { case (importance, name) =>
          println(s"$name: $importance")
        }
      case x =>
        println(s"Don't know $x of type ${x.getClass}")
    }
  }
}

object MetricHelper {

  def showMetrics(estimatorName: String, hashCode: Int, scoreAndLabels: RDD[(Double, Double)]): Double = {
    val binaryMetrics = new BinaryClassificationMetrics(scoreAndLabels)
    // AUPRC
    val auPRC = binaryMetrics.areaUnderPR
    // AUROC
    val auROC = binaryMetrics.areaUnderROC

    // F-measure
    val f1Score = binaryMetrics.fMeasureByThreshold.map { case (t, f) =>
      s"Threshold: $t, F-score: $f, Beta = 1"
    }.collect()

    val multiClassMetrics = new MulticlassMetrics(scoreAndLabels)
    val accuracy = multiClassMetrics.accuracy

    val confusionMatrixTable: String = getConfusionMatrixTable(multiClassMetrics)
    val fp = multiClassMetrics.confusionMatrix(0, 1)

    val str =
      s"""Estimator: $estimatorName [$hashCode]
         |\tFalse Positive: ${fp / scoreAndLabels.count() * 100}%%
         |\tF-measure:
         |\t\t${f1Score.mkString("\n\t\t")}
         |\tArea under precision-recall curve = $auPRC
         |\tArea under ROC = $auROC
         |\tConfusion matrix:
         |%s
         |\tAccuracy = $accuracy""".stripMargin.format("\t\t" + confusionMatrixTable.replace("\n", "\n\t\t"))

    println(str) //scalastyle:ignore
    accuracy
  }

  def getConfusionMatrixTable(multiClassMetrics: MulticlassMetrics): String = {
    val cm = multiClassMetrics.confusionMatrix
    val tn = cm(0, 0)
    val fp = cm(0, 1)
    val fn = cm(1, 0)
    val tp = cm(1, 1)

    val firstColumn = Array[String]("Actual = 0", tn.toString, fp.toString)
    val secondColumn = Array[String]("Actual = 1", fn.toString, tp.toString)

    val fcl = firstColumn.maxBy(x => x.length).length - 1 // -1 because we added extra space in template
    val scl = secondColumn.maxBy(x => x.length).length - 1

    val confusionMatrixTableTemplate =
      "+---------------+------------+------------+\n" +
        "| ############# | Actual = 1 | Actual = 0 |\n" +
        "+---------------+------------+------------+\n" +
        s"| Predicted = 1 | %-${fcl}d  | %-${scl}d  |\n" +
        "+---------------+------------+------------+\n" +
        s"| Predicted = 0 | %-${fcl}d  | %-${scl}d  |\n" +
        "+---------------+------------+------------+"

    val confusionMatrixTable = confusionMatrixTableTemplate.format(tp.toLong, fp.toLong, fn.toLong, tn.toLong)
    confusionMatrixTable
  }

  def getFeatureImportance(gbtModel: LightGBMClassificationModel, allColumns: Array[String], importanceType: String): Array[(Double, String)] = {
    val featureImportance = gbtModel.getFeatureImportances(importanceType).zipWithIndex.map { case (importance, idx) => (importance, allColumns.lift(idx).getOrElse("")) }
    featureImportance.filter { case (importance, _) => importance > 0 }.sortBy(x => -x._1)
  }
}