package nl.ordina.bigdata.myo.strategy

import nl.ordina.bigdata.myo.Constants
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, DataFrame}

class UnaggregatedMyoStrategy extends MyoStrategy {
  private var schema: StructType = null

  override def createDataFrame(path: String, sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val dataFrame: DataFrame = sqlContext.read.json(path + "/raw-myo-data/*.json")
    this.schema = dataFrame.schema
    dataFrame
  }


  override def trainModel(dataFrame: DataFrame): CrossValidatorModel = {
    val labelIndexer = new StringIndexer().setInputCol(Constants.LABEL).setOutputCol("indexedLabel").fit(dataFrame)

    val assembler = new VectorAssembler().setInputCols(Constants.FEATURES).setOutputCol("features")
    val dt = new DecisionTreeRegressor().setLabelCol("indexedLabel").setFeaturesCol("features").setPredictionCol("prediction")
    val pipeline = new Pipeline().setStages(Array(labelIndexer, assembler, dt))

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxBins, Array(5, 10, 25))
      .addGrid(dt.maxDepth, Array(5, 10, 25))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    cv.fit(dataFrame)
  }

  override def displayPrediction(rdd: RDD[String], model: CrossValidatorModel, sqlContext: SQLContext): Unit = {
    val dataFrame = sqlContext.read.schema(schema).json(rdd)
    val predictionDataFrame = model.transform(dataFrame)
    predictionDataFrame.groupBy("prediction").count().show()
  }

}