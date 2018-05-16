package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by njayakumar on 5/16/2018.
  */
object inference_engine_master {

  val TASK: String = "inference_engine_master"

  def getData(spark: SparkSession,  current_time: Timestamp): DataFrame = {
    null
  }
}
