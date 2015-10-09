package nl.ordina.bigdata.myo

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object MyoMain {

  def main(args: Array[String]): Unit = {
    //Initialize contexts
    val conf = new SparkConf().setMaster("local[6]").setAppName("myo-streaming")
    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)
    val streamingContext = new StreamingContext(sc, Seconds(1))

    //Start training part
    val dataFrame = createDataFrame(Constants.dataPath, sc, sqlContext)
    implicit val schema = dataFrame.schema
    implicit val model = trainModel(dataFrame)

    //Start streaming part
    val dstream = streamingContext.socketTextStream("localhost", Constants.dataServerPort)
    dstream.foreachRDD(rdd => displayPrediction(rdd))
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def aggregateData(data: RDD[String]): RDD[Array[Double]] = ???

  def createDataFrame(path: String, sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val data = sc.textFile(path)
    val aggregatedData = aggregateData(data)
    val rowRDD = aggregatedData.map(toMyoRow)

    val header = Constants.header
    val schema = determineSchemaFromHeader(header)
    sqlContext.createDataFrame(rowRDD, schema)
  }

  def determineSchemaFromHeader(header: Array[String]): StructType = {
    StructType(header.map(name => StructField(name, DoubleType, nullable = false)))
  }

  def toMyoRow(line: Array[Double]): Row = {
    Row.fromSeq(line)
  }

  def trainModel(dataFrame: DataFrame): CrossValidatorModel = {
    val splits = dataFrame.randomSplit(Array(0.7, 0.3))
    val (trainingData, testingData) = (splits(0), splits(1))

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(trainingData)
    val assembler = new VectorAssembler().setInputCols(Constants.header).setOutputCol("features")
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

    cv.fit(trainingData)
  }

  /**
   * This method should display the prediction using some form of output. Currently, it just prints the predicted label
   * as a test implementation to the standard out.
   */
  def displayPrediction(rdd: RDD[String])(implicit model: CrossValidatorModel, sqlContext: SQLContext, schema: StructType): Unit = {
    val row = aggregateData(rdd).map(toMyoRow)
    val dataFrame = sqlContext.createDataFrame(row, schema)
    val predictionDataFrame = model.transform(dataFrame)
    predictionDataFrame.foreach(row => println(s"prediction is ${row.get(row.fieldIndex("prediction"))}"))
  }
}
