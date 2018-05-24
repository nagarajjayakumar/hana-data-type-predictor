package com.hortonworks.faas.spark.predictor.schema_crawler.task

import java.sql.Timestamp

import com.hortonworks.faas.spark.predictor.inference_engine.analytic.common.analytic.AdvancedAnalyticType
import com.hortonworks.faas.spark.predictor.schema_crawler.model.HanaActiveObject
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by njayakumar on 5/16/2018.
  */
object schema_crawler_master {

  val TASK: String = "schema_crawler_master"

  val ANALYTIC_TYPE = AdvancedAnalyticType.HANA

  val SRCDBONAME = "default/default"

  def getHanaMetaData(spark: SparkSession,
              namespace: String = "default",
              dboname: String = "default",
              current_time: Timestamp): Dataset[HanaActiveObject] = {

    hana_active_object.getData(spark, namespace, dboname, current_time)

 }



}
