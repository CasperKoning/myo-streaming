package nl.ordina.bigdata.myo.strategy

import nl.ordina.bigdata.myo.Constants
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.{RegressionEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkException}

class UnaggregatedMyoStrategy extends MyoStrategy {
  private var schema: StructType = null

  override def createDataFrame(path: String, sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val dataFrame: DataFrame = sqlContext.read.json(path + "/myo-data-with-label/*.json")
    this.schema = dataFrame.schema
    dataFrame
  }

  override def trainModel(dataFrame: DataFrame): CrossValidatorModel = {
    dataFrame.cache()
    val assembler = new VectorAssembler().setInputCols(Constants.FEATURES).setOutputCol("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(24)

    val dt = new DecisionTreeRegressor()
      .setLabelCol(Constants.LABEL)
      .setFeaturesCol("pcaFeatures")
      .setPredictionCol("prediction")
      .setImpurity("variance")

    val pipeline = new Pipeline().setStages(Array(assembler, pca, dt))

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxDepth, Array(20))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator().setLabelCol(Constants.LABEL).setPredictionCol("prediction").setMetricName("rmse"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    cv.fit(dataFrame)
  }

  override def displayPrediction(rdd: RDD[String], model: CrossValidatorModel, sqlContext: SQLContext): Unit = {
    val dataFrame = sqlContext.read.schema(schema).json(rdd)
    try {
      val predictionsDataFrame = model.transform(dataFrame)
      val aggregated = predictionsDataFrame.agg(avg(predictionsDataFrame("prediction")))
      aggregated.show()
    } catch {
      case e: SparkException => println("Op een of andere manier is er een lege string naar Spark gestuurd. Hierdoor kunnen we deze RDD niet verwerken. Slaan we dus over.")
    }
  }

}