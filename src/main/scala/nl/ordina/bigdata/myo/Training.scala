package nl.ordina.bigdata.myo

import java.io.{FileOutputStream, ObjectOutputStream}

import nl.ordina.bigdata.myo.strategy.{AggregatedMyoStrategy, UnaggregatedMyoStrategy}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Training {
  def main(args: Array[String]): Unit = {
    //Initialize contexts
    val conf = new SparkConf().setMaster("local[6]").setAppName("myo-streaming")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    //Initialize MyoStrategy
    val myoStrategy = args(0) match {
      case "aggregated" => new AggregatedMyoStrategy
      case "unaggregated" => new UnaggregatedMyoStrategy
      case _ => throw new IllegalArgumentException(s"${args(0)} is not a valid strategy. Supported strategies are: 'aggregated' and 'unaggregated'.")
    }

    //Start training part
    val dataFrame = myoStrategy.createDataFrame(Constants.DATA_PATH, sc, sqlContext)
    val splits = dataFrame.randomSplit(Array(0.7, 0.3),42)
    val (trainingDataFrame, testingDataFrame) = (splits(0), splits(1))
    val trainedModel = myoStrategy.trainModel(trainingDataFrame)

    //Write model to disk
    val oos = new ObjectOutputStream(new FileOutputStream(Constants.MODEL_PATH))
    oos.writeObject(trainedModel)
    oos.close()

    //Evaluate predictions
    val predictions = trainedModel.transform(testingDataFrame)
    val evaluator = myoStrategy.getEvaluator()
    val precision = evaluator.evaluate(predictions)
    println(s"Precision is  $precision")
  }

}
