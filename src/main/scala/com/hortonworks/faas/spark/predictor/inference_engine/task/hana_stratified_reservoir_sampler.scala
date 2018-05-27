package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp

import com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngineOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

object hana_stratified_reservoir_sampler {


  val TASK: String = "hana_stratified_reservoir_sampler"

  val hana_active_object_query: String = "select * from \"_SYS_REPO\".\"ACTIVE_OBJECT\" where lower(object_suffix) in ('calculationview', 'attributeview', 'analyticview')  "


  def inferSchema(spark: SparkSession,
                  opts: InferenceEngineOptions,
                  current_time: Timestamp): DataFrame = {


    val dboname = opts.src_dbo_name
    val Array(package_id, object_name, _*) = dboname.split("/")
    val whereClause = "and package_id like '".concat(package_id).concat("%' and object_name like '").concat(object_name).concat("%'")
    val sql = hana_active_object_query.concat(whereClause)
    val df = spark
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map("query" -> (sql)))
      .load()

    df

  }


}
