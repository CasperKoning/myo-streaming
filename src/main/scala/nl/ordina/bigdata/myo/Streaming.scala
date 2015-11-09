package nl.ordina.bigdata.myo

import java.io.{OutputStream, PrintStream, ObjectInputStream, FileInputStream}
import java.net.ServerSocket

import nl.ordina.bigdata.myo.strategy.{AggregatedMyoStrategy, UnaggregatedMyoStrategy}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Seconds}

object Streaming {
  def main(args: Array[String]): Unit = {
    //Initialize contexts
    val conf = new SparkConf().setMaster("local[6]").setAppName("myo-streaming")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    val streamingContext = new StreamingContext(sc, Milliseconds(500))

    //Initialize MyoStrategy
    val myoStrategy = args(0) match {
      case "aggregated" => new AggregatedMyoStrategy
      case "unaggregated" => new UnaggregatedMyoStrategy
      case _ => throw new IllegalArgumentException(s"${args(0)} is not a valid strategy. Supported strategies are: 'aggregated' and 'unaggregated'.")
    }

    val dataFrame = sqlContext.read.json(Constants.DATA_PATH + "/myo-data-with-label-classification/*.json")
    val schema = dataFrame.schema
    val ois = new ObjectInputStream(new FileInputStream(Constants.MODEL_PATH))
    val model = ois.readObject().asInstanceOf[CrossValidatorModel]

    val dstream = streamingContext.socketTextStream("localhost", Constants.DATA_SERVER_PORT)
    dstream.foreachRDD(rdd => myoStrategy.displayPrediction(rdd, model, sqlContext, schema))
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
