package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp

import com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngineOptions
import com.hortonworks.faas.spark.predictor.inference_engine.analytic.common.analytic.AdvancedAnalyticType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by njayakumar on 5/16/2018.
  */
object inference_engine_master {

  val TASK: String = "inference_engine_master"

  val ANALYTIC_TYPE = AdvancedAnalyticType.HANA

  val SRCDBONAME = "default/default"

  val SAMPLING_TECHNIQ = "full"

  val SAMPLING_PERCENTAGE = "1"

  val RUNTIME_ENV = "local"

  
  def getData(spark: SparkSession, opts: InferenceEngineOptions, current_time: Timestamp): DataFrame = {
     spark
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map("query" -> ("SELECT * FROM \"" + "" + "\"." + "\"DataLake.Deltaviews.TransactionViews/InstallationOwnershipTS\" " )))
      // .options(Map("query" -> ("SELECT * FROM \"" + dbName + "\"." + "DFKKOPK" )))
      .load()
  }
}
