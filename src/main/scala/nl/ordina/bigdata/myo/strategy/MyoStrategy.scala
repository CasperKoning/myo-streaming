package nl.ordina.bigdata.myo.strategy

import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

trait MyoStrategy {
  def createDataFrame(path: String): DataFrame

  def trainModel(dataFrame: DataFrame): CrossValidatorModel

  def displayPrediction(rdd: RDD[String], model: CrossValidatorModel): Unit
}
