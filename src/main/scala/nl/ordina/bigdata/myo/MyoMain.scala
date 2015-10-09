package nl.ordina.bigdata.myo

import nl.ordina.bigdata.myo.strategy.{MyoStrategy, UnaggregatedMyoStrategy, AggregatedMyoStrategy}
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

    //Initialize MyoStrategy
    val myoStrategy = args(0) match {
      case "aggregated" => new AggregatedMyoStrategy(sc, sqlContext)
      case "unaggregated" => new UnaggregatedMyoStrategy(sc, sqlContext)
      case _ => throw new IllegalArgumentException(s"${args(0)} is not a valid strategy. Supported strategies are: 'aggregated' and 'unaggregated'.")
    }

    //Start training part
    val dataFrame = myoStrategy.createDataFrame(Constants.dataPath)
    val model = myoStrategy.trainModel(dataFrame)

    //Start streaming part
    val dstream = streamingContext.socketTextStream("localhost", Constants.dataServerPort)
    dstream.foreachRDD(rdd => myoStrategy.displayPrediction(rdd,model))
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
