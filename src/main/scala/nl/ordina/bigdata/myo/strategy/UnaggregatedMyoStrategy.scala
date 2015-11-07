package nl.ordina.bigdata.myo.strategy

import nl.ordina.bigdata.myo.Constants
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.{Evaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{PCA, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkException}

class UnaggregatedMyoStrategy extends MyoStrategy {
  override def createDataFrame(path: String, sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    sqlContext.read.json(path + "/myo-data-with-label-classification/*.json")
  }

  override def trainModel(dataFrame: DataFrame): CrossValidatorModel = {
    dataFrame.cache()
    val indexer = new StringIndexer() //Indexed labels are ordered by frequency, with the most frequent label getting value 0.0 and the least frequent label getting value numClasses-1
      .setInputCol("label")
      .setOutputCol("indexedLabel")

    //0.0 =
    //1.0 =
    //2.0 =

    val assembler = new VectorAssembler()
      .setInputCols(Constants.FEATURES_UNAGGREGATED)
      .setOutputCol("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(24)

    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("pcaFeatures")
      .setPredictionCol("prediction")
      .setImpurity("gini")

    val pipeline = new Pipeline()
      .setStages(Array(indexer, assembler, pca, dt))

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxDepth, Array(10, 20))
      .build()

    val evaluator = getEvaluator()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    cv.fit(dataFrame)
  }

  override def displayPrediction(rdd: RDD[String], model: CrossValidatorModel, sqlContext: SQLContext, schema: StructType): Unit = {
    try {
      val dataFrame = sqlContext.read.schema(schema).json(rdd).drop("label") //drop label in order to prevent failure of the stringindexer model in the pipeline
      val predictionsDataFrame = model.transform(dataFrame)
      val counts = predictionsDataFrame.map(row => row.getAs[Double]("prediction")).countByValue()
      val sorted = counts.toList.sorted(new Ordering[(Double, Long)] {
        override def compare(x: (Double, Long), y: (Double, Long)): Int = (y._2 - x._2).asInstanceOf[Int]
      })
      if (sorted.nonEmpty) {
        val topClass = sorted.head._1
        println(counts)
        println(topClass)
      }
    } catch {
      case e: SparkException => print("") //println("Op een of andere manier is er een lege string naar Spark gestuurd. Hierdoor kunnen we deze RDD niet verwerken. Slaan we dus over.")
    }
  }

  override def getEvaluator(): Evaluator = {
    new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    //    new RegressionEvaluator()
    //      .setLabelCol("label")
    //      .setPredictionCol("prediction")
    //      .setMetricName("rmse")
  }
}