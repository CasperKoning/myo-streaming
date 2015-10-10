package nl.ordina.bigdata.myo.strategy

import org.apache.spark.SparkContext
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

class AggregatedMyoStrategy extends MyoStrategy {
  override def createDataFrame(path: String, sc: SparkContext, sqlContext: SQLContext): DataFrame = ???

  override def trainModel(dataFrame: DataFrame): CrossValidatorModel = ???

  override def displayPrediction(rdd: RDD[String], model: CrossValidatorModel, sqlContext: SQLContext): Unit = ???
}
