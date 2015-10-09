package nl.ordina.bigdata.myo.strategy

import nl.ordina.bigdata.myo.Constants
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, CrossValidatorModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

class UnaggregatedMyoStrategy(val sc: SparkContext, val sqlContext: SQLContext) extends MyoStrategy {
  override def createDataFrame(path: String): DataFrame = {
    val data = sc.textFile(path)
    val rowRDD = data.map(toUnaggregatedMyoRow)

    val header = Constants.unaggregatedHeader
    val schema = determineSchemaFromHeader(header)
    sqlContext.createDataFrame(rowRDD, schema)
  }

  override def trainModel(dataFrame: DataFrame): CrossValidatorModel = {
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(dataFrame)
    val assembler = new VectorAssembler().setInputCols(Constants.unaggregatedHeader).setOutputCol("features")
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
      .setNumFolds(2)

    cv.fit(dataFrame)
  }

  override def displayPrediction(rdd: RDD[String], model: CrossValidatorModel): Unit = {
    //    val row = aggregateData(rdd).map(toUnaggregatedMyoRow)
    //    val dataFrame = sqlContext.createDataFrame(row, schema)
    //    val predictionDataFrame = model.transform(dataFrame)
    //    predictionDataFrame.foreach(row => println(s"prediction is ${row.get(row.fieldIndex("prediction"))}"))
  }


  def determineSchemaFromHeader(header: Array[String]): StructType = {
    StructType(header.map(name => StructField(name, DoubleType, nullable = false)))
  }

  def toUnaggregatedMyoRow(line: String): Row = {
    val splits = line.split(";")
    Row.fromSeq(splits.map(_.toDouble))
  }

}
