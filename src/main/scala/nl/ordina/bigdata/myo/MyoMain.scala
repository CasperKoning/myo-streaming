package nl.ordina.bigdata.myo

import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object MyoMain {

  def main(args: Array[String]): Unit = {
    //Initialize contexts
    val conf = new SparkConf().setMaster("local[6]").setAppName("myo-streaming")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val streamingContext = new StreamingContext(sc, Seconds(1))

    //Start training part
    val dataFrame = createDataFrame(Constants.dataPath, sc, sqlContext)
    implicit val model: DecisionTreeModel = trainModel(dataFrame)

    //Start streaming part
    val dstream = streamingContext.socketTextStream("localhost", Constants.dataServerPort)
    dstream.foreachRDD(rdd => rdd.foreach(displayPrediction))
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def createDataFrame(path: String, sc: SparkContext, sqlContext: SQLContext): DataFrame = {
    val data = sc.textFile(path)
    val headerPart = data.first()
    val dataPart = data.filter(line => !line.startsWith("mean"))
    val schema = determineSchemaFromHeader(headerPart)
    val rowRDD = dataPart.map(line => Row(line.split(";"))) //Want to leave out the header, so we filter it out
    sqlContext.createDataFrame(rowRDD, schema)
  }

  def determineSchemaFromHeader(header: String): StructType = {
    val splits = header.split(";")
    val label = splits.reverse.head //last entry
    val features = splits.reverse.tail

    val labelStruct = StructField(label, StringType, nullable = false)
    val featuresStruct = features.map(featureName => StructField(featureName, DoubleType, nullable = false)) //all doubles
    StructType((labelStruct :: featuresStruct.toList).reverse)
  }

  //TODO
  /**
   *
   *
   * This method should train a decision tree model. Currently, this has no implementation.
   */
  def trainModel(dataFrame: DataFrame): DecisionTreeModel = {
    null
  }

  //TODO
  /**
   * This method should display the prediction using some form of output. Currently, it just prints the label as a test
   * implementation.
   */
  def displayPrediction(line: String)(implicit model: DecisionTreeModel): Unit = {
    val label = line.split(";").reverse.head
    println(label)
  }
}
