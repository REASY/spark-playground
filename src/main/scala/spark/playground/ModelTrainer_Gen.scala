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
  val allColumns: Array[String] = Array("lid", "vehOnRoad", "length", "capacity", "lanes") ++ outColumns ++ inColumns

  val LinkId = "51110"
  val seed: Long = 41L
  val someDataPoints: Array[String] = Array(
    "32000", "70506", "70084", "13400", "57838", "55972", "22669", "72854", "31386", "27883", "55946", "64994", "40122", "25038")

  val moreThan1kDataPoints: Array[String] = Array(
    "32000", "70506", "70084", "13400", "57838", "55972", "22669", "72854", "31386", "27883", "55946", "64994", "40122", "25038",
    "51408", "71354", "34612", "78984", "90004", "90488", "88932", "95696", "64916", "71391", "40116", "50804", "66534", "55327",
    "36380",
    "70082",
    "27881",
    "91612",
    "91616",
    "94928",
    "69992",
    "40117",
    "55976",
    "11765",
    "40112",
    "39668",
    "124",
    "34620",
    "6776",
    "71396",
    "88924",
    "22661",
    "55323",
    "31422",
    "92562",
    "71392",
    "43594",
    "11766",
    "51358",
    "22285",
    "69218",
    "72852",
    "22668",
    "31376",
    "40125",
    "87674",
    "71390",
    "55942",
    "51133",
    "22660",
    "71395",
    "89637",
    "31986",
    "22283",
    "65778",
    "4638",
    "40266",
    "52309",
    "54466",
    "45677",
    "45670",
    "89825",
    "89800",
    "89893",
    "57416",
    "8550",
    "85818",
    "64658",
    "32002",
    "56100",
    "41360",
    "71802",
    "89772",
    "56168",
    "40280",
    "36358",
    "55339",
    "49610",
    "49258",
    "47428",
    "75314",
    "49198",
    "47490",
    "2916",
    "89812",
    "65568",
    "71394",
    "70862",
    "52314",
    "55978",
    "49262",
    "55345",
    "69982",
    "70630",
    "39470",
    "36360",
    "66466",
    "71393",
    "23418",
    "31990",
    "31956",
    "65118",
    "90008",
    "31388",
    "93878",
    "69892",
    "65330",
    "91122",
    "91592",
    "95606",
    "43620",
    "40119",
    "49966",
    "65328",
    "56090",
    "45675",
    "64898",
    "69944",
    "31372",
    "56092",
    "32748",
    "43596",
    "90256",
    "11758",
    "40264",
    "55337",
    "52315",
    "52306",
    "51472",
    "92124",
    "22666",
    "69538",
    "51142",
    "51362",
    "55343",
    "31378",
    "71388",
    "27879",
    "64904",
    "89885",
    "69986",
    "89781",
    "70591",
    "92214",
    "36378",
    "49676",
    "47500",
    "51360",
    "6782",
    "70070",
    "27885",
    "11760",
    "36368",
    "22663",
    "89965",
    "66040",
    "52308",
    "35560",
    "52312",
    "31384",
    "87670",
    "90528",
    "70434",
    "89905",
    "89813",
    "40123",
    "47502",
    "89896",
    "55347",
    "89845",
    "87004",
    "53906",
    "70214",
    "22658",
    "65256",
    "11770",
    "31998",
    "25628",
    "48404",
    "6410",
    "71790",
    "47498",
    "78986",
    "32914",
    "89840",
    "69870",
    "40115",
    "40113",
    "71387",
    "89792",
    "32752",
    "40124",
    "40278",
    "27887",
    "64912",
    "51412",
    "48416",
    "90016",
    "90000",
    "50806",
    "43622",
    "41362",
    "6778",
    "78770",
    "48418",
    "55335",
    "11764",
    "49674",
    "35562",
    "71386",
    "55944",
    "43624",
    "56202",
    "89915",
    "91588",
    "71356",
    "36848",
    "47506",
    "51406",
    "43276",
    "65424",
    "66470",
    "51364",
    "31370",
    "31374",
    "95376",
    "47440",
    "40111",
    "45681",
    "49712",
    "55974",
    "40120",
    "25626",
    "6408",
    "53166",
    "65268",
    "53168",
    "36366",
    "55333",
    "26469",
    "89873",
    "75180",
    "47442",
    "57840",
    "22544",
    "66536",
    "89877",
    "70504",
    "34346",
    "65570",
    "89757",
    "40114",
    "37522",
    "48420",
    "13402",
    "92131",
    "91580",
    "43598",
    "34745",
    "70086",
    "51146",
    "70468",
    "31416",
    "40121",
    "6774",
    "49260",
    "69762",
    "22662",
    "54394",
    "31994",
    "6780",
    "78768",
    "78772",
    "48414",
    "89801",
    "19682",
    "89930",
    "78766",
    "65395",
    "64726",
    "52311",
    "32918",
    "52158",
    "47496",
    "65506",
    "71650",
    "11759",
    "49608",
    "64910",
    "32920",
    "32750",
    "47504",
    "56200",
    "69126",
    "82340",
    "89897",
    "55980",
    "47492",
    "55329",
    "55325",
    "11761",
    "79158",
    "52160",
    "74714",
    "49612",
    "69764",
    "34744",
    "36622",
    "89784",
    "39670",
    "22659",
    "69984",
    "36618",
    "36620",
    "56130",
    "25040",
    "36636",
    "95292",
    "53902",
    "52310",
    "65422",
    "64914",
    "57582",
    "65258",
    "83046",
    "89892",
    "36382",
    "64996",
    "51131",
    "22664",
    "31988",
    "90532",
    "11769",
    "89869",
    "19194",
    "34622",
    "51135",
    "52313",
    "91272",
    "57418",
    "87956",
    "11771",
    "11767",
    "39672",
    "82356",
    "89785",
    "69872",
    "53904",
    "78982",
    "31380",
    "31996",
    "40276",
    "53164",
    "90012",
    "6412",
    "40262",
    "92135",
    "65614",
    "72842",
    "31418",
    "32916",
    "31544",
    "31970",
    "89750",
    "45671",
    "31382",
    "69988",
    "55341",
    "42972",
    "90484",
    "51136",
    "45679",
    "22552",
    "73580",
    "52806",
    "93908",
    "32294",
    "71352",
    "70450",
    "93858",
    "11768",
    "89876",
    "31992",
    "40110",
    "93854",
    "55982",
    "70841",
    "41463",
    "75800",
    "31402",
    "6772",
    "91264",
    "89914",
    "69980",
    "71804",
    "41461",
    "55331",
    "89841",
    "49765",
    "71706",
    "89996",
    "31420",
    "64906",
    "47494",
    "71708",
    "89872",
    "65899",
    "78762",
    "57584",
    "89773",
    "50500",
    "69780",
    "91260",
    "43626",
    "52307",
    "37154",
    "78764",
    "65572",
    "89849",
    "51140",
    "32744",
    "40118",
    "92120",
    "4634",
    "4636",
    "70436",
    "73582"
  )

  def main(args: Array[String]): Unit = {
    val mapping = spark.read.format("csv").option("header", "true").load("C:\\temp\\BeamRegression\\LinkInOut_mapping.csv").cache()
    val df = readCsv("C:\\temp\\BeamRegression\\out_link_stat.csv")
      .filter(col("linkId").isin(someDataPoints: _*))
      .join(mapping, Seq("linkId"), "inner")
      .withColumn("capacity", col("capacity").cast(sql.types.DoubleType))
      .withColumn("lanes", col("lanes").cast(sql.types.DoubleType))
      .cache()

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
      .setMaxIter(10)

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
      .withColumn("length", col("length").cast(sql.types.DoubleType))
    outColumns.foreach { c =>
      df = df.withColumn(c, col(c).cast(sql.types.DoubleType))
    }
    inColumns.foreach { c =>
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
}
