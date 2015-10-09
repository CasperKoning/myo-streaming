package nl.ordina.bigdata.myo.strategy

import nl.ordina.bigdata.myo.Constants
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class UnaggregatedMyoStrategy(val sc: SparkContext, val sqlContext: SQLContext) extends MyoStrategy {
  val schema = determineSchemaFromHeader(Constants.UNAGGREGATED_HEADER)

  override def createDataFrame(path: String): DataFrame = {
    val data = sc.textFile(path + "/raw-myo-data/*.csv")
    val rowRDD = data.map(toMyoRow)
    sqlContext.createDataFrame(rowRDD, schema)
  }

  override def trainModel(dataFrame: DataFrame): CrossValidatorModel = {
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(dataFrame)
    val assembler = new VectorAssembler().setInputCols(Constants.UNAGGREGATED_HEADER).setOutputCol("features")
    val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setPredictionCol("prediction").setImpurity("gini")
    val pipeline = new Pipeline().setStages(Array(labelIndexer, assembler, dt))

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxBins, Array(5, 10, 25))
      .addGrid(dt.maxDepth, Array(5, 10, 25))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    cv.fit(dataFrame)
  }

  override def displayPrediction(rdd: RDD[String], model: CrossValidatorModel): Unit = {
    val row = rdd.map(toMyoRow)
    val dataFrame = sqlContext.createDataFrame(row, schema)
    val predictionDataFrame = model.transform(dataFrame)
    predictionDataFrame.groupBy("prediction").count().show()
  }

  def determineSchemaFromHeader(header: Array[String]): StructType = {
    StructType(header.map(name => StructField(name, DoubleType, nullable = false)))
  }

  private[this] def toMyoRow(line: String): Row = {
    val splits = line.split(";")
    Row.fromSeq(splits.map(_.toDouble))
  }
}
