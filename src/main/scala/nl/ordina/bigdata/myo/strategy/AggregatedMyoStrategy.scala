package nl.ordina.bigdata.myo.strategy

import java.io.File

import nl.ordina.bigdata.myo.Constants
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.{Evaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{PCA, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkException}

class AggregatedMyoStrategy extends MyoStrategy {
  override def createDataFrame(path: String, sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val directory = new File(path + "/myo-data-with-label-classification")
    val files = directory.listFiles().filter(_.isFile).filter(f => """.*\.json$""".r.findFirstIn(f.getName).isDefined)
    val aggregatedDataFrames = for (file <- files) yield {
      val dataFrame = sqlContext.read.json(file.getAbsolutePath).filter("gyro_x != 0.0")
      aggregateData(dataFrame)
    }
    aggregatedDataFrames.reduce(_ unionAll _)
  }

  def aggregateData(dataFrame: DataFrame): DataFrame = {
    dataFrame.agg(
      first("label"),
      min("emg_0"), min("emg_1"), min("emg_2"), min("emg_3"), min("emg_4"), min("emg_5"), min("emg_6"), min("emg_7"),
      avg("emg_0"), avg("emg_1"), avg("emg_2"), avg("emg_3"), avg("emg_4"), avg("emg_5"), avg("emg_6"), avg("emg_7"),
      max("emg_0"), max("emg_1"), max("emg_2"), max("emg_3"), max("emg_4"), max("emg_5"), max("emg_6"), max("emg_7"),
      min("normalizedQuaternion_x"), min("normalizedQuaternion_y"), min("normalizedQuaternion_z"), min("normalizedQuaternion_w"),
      avg("normalizedQuaternion_x"), avg("normalizedQuaternion_y"), avg("normalizedQuaternion_z"), avg("normalizedQuaternion_w"),
      max("normalizedQuaternion_x"), max("normalizedQuaternion_y"), max("normalizedQuaternion_z"), max("normalizedQuaternion_w"),
      min("quaternion_x"), min("quaternion_y"), min("quaternion_z"), min("quaternion_w"),
      avg("quaternion_x"), avg("quaternion_y"), avg("quaternion_z"), avg("quaternion_w"),
      max("quaternion_x"), max("quaternion_y"), max("quaternion_z"), max("quaternion_w"),
      min("acceleratorMeter_x"), min("acceleratorMeter_y"), min("acceleratorMeter_z"),
      avg("acceleratorMeter_x"), avg("acceleratorMeter_y"), avg("acceleratorMeter_z"),
      max("acceleratorMeter_x"), max("acceleratorMeter_y"), max("acceleratorMeter_z"),
      min("normalizedAcceleratorMeter_x"), min("normalizedAcceleratorMeter_y"), min("normalizedAcceleratorMeter_z"),
      avg("normalizedAcceleratorMeter_x"), avg("normalizedAcceleratorMeter_y"), avg("normalizedAcceleratorMeter_z"),
      max("normalizedAcceleratorMeter_x"), max("normalizedAcceleratorMeter_y"), max("normalizedAcceleratorMeter_z"),
      min("gyro_x"), min("gyro_y"), min("gyro_z"),
      avg("gyro_x"), avg("gyro_y"), avg("gyro_z"),
      max("gyro_x"), max("gyro_y"), max("gyro_z"),
      min("normalizedGyro_x"), min("normalizedGyro_y"), min("normalizedGyro_z"),
      avg("normalizedGyro_x"), avg("normalizedGyro_y"), avg("normalizedGyro_z"),
      max("normalizedGyro_x"), max("normalizedGyro_y"), max("normalizedGyro_z"),
      min("eulerAngels_pitch"), min("eulerAngels_roll"), min("eulerAngels_yaw"),
      avg("eulerAngels_pitch"), avg("eulerAngels_roll"), avg("eulerAngels_yaw"),
      max("eulerAngels_pitch"), max("eulerAngels_roll"), max("eulerAngels_yaw"),
      min("normalizedEulerAngels_pitch"), min("normalizedEulerAngels_roll"), min("normalizedEulerAngels_yaw"),
      avg("normalizedEulerAngels_pitch"), avg("normalizedEulerAngels_roll"), avg("normalizedEulerAngels_yaw"),
      max("normalizedEulerAngels_pitch"), max("normalizedEulerAngels_roll"), max("normalizedEulerAngels_yaw")
    )
  }

  override def trainModel(dataFrame: DataFrame): CrossValidatorModel = {
    dataFrame.cache()

    val indexer = new StringIndexer()
      .setInputCol("FIRST(label)")
      .setOutputCol("indexedLabel")

    val assembler = new VectorAssembler()
      .setInputCols(Constants.FEATURES_AGGREGATED)
      .setOutputCol("features")

    val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(60)

    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("pcaFeatures")
      .setPredictionCol("prediction")
      .setImpurity("gini")

    val pipeline = new Pipeline().setStages(Array(indexer, assembler, pca, dt))

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxDepth, Array(5, 10, 20, 30))
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
    val dataFrame = sqlContext.read.schema(schema).json(rdd).drop("label").filter("gyro_x != 0.0")
    val aggregatedDataFrame = aggregateData(dataFrame)
    try {
      val predictionDataFrame = model.transform(aggregatedDataFrame)
      predictionDataFrame.select("prediction").show()
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
    //      .setLabelCol("avg(label)")
    //      .setPredictionCol("prediction")
    //      .setMetricName("rmse")
  }
}
