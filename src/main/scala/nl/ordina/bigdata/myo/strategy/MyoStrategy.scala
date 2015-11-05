package nl.ordina.bigdata.myo.strategy

import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

trait MyoStrategy extends Serializable {
  def getEvaluator(): Evaluator

  def createDataFrame(path: String, sc: SparkContext, sqlContext: SQLContext): DataFrame

  def trainModel(dataFrame: DataFrame): CrossValidatorModel

  def displayPrediction(rdd: RDD[String], model: CrossValidatorModel, sqlContext: SQLContext, schema: StructType): Unit
}
