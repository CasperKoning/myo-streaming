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

  def createDataFrame(path: String, sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val data = sc.textFile(path)
    val headerPart = data.first()
    val dataPart = data.filter(line => !line.startsWith("label")) //Want to leave out the header, so we filter it out
    val schema = determineSchemaFromHeader(headerPart)
    val rowRDD = dataPart.map(toMyoRow)
    sqlContext.createDataFrame(rowRDD, schema)
  }

  def determineSchemaFromHeader(header: String): StructType = {
    val splits = header.split(";")
    StructType(splits.map(name => StructField(name, DoubleType, nullable = false)))
  }

  def toMyoRow(line: String): Row = {
    val splits = line.split(";")
    Row.fromSeq(splits.map(_.toDouble))
  }

  def trainModel(dataFrame: DataFrame): CrossValidatorModel = {
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(dataFrame)
    val assembler = new VectorAssembler().setInputCols(Constants.featureCols).setOutputCol("features")
    val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setPredictionCol("prediction").setImpurity("gini")
    val pipeline = new Pipeline().setStages(Array(labelIndexer, assembler, dt))

    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxBins, Array(5, 10, 20))
      .addGrid(dt.maxDepth, Array(5, 10, 20))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    cv.fit(dataFrame)
  }

  /**
   * This method should display the prediction using some form of output. Currently, it just prints the predicted label
   * as a test implementation to the standard out.
   */
  def displayPrediction(rdd: RDD[String])(implicit model: CrossValidatorModel, sqlContext: SQLContext, schema: StructType): Unit = {
    val dataFrame = sqlContext.createDataFrame(rdd.map(toMyoRow),schema)
    val predictionDataFrame = model.transform(dataFrame)
    predictionDataFrame.foreach(row => println(s"prediction is ${row.get(row.fieldIndex("prediction"))}"))
  }
}
