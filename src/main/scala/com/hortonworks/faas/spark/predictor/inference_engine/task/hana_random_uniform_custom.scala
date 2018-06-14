package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp

import com.google.common.base.Joiner
import com.hortonworks.faas.spark.connector.util.InferSchema
import com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngineOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object hana_random_uniform_custom extends Logging {


  log
  val TASK: String = "hana_random_uniform_custom"

  /*

    for large tables .. it is tough to get the sample data ..

    first we need to get the count of the targeted view ..

    Calculate the percentage and do the top query for the random uniform sampling

   */


  def getData(spark: SparkSession,
              opts: InferenceEngineOptions,
              dbaoDetails: List[SourceDbActiveObjectDetail],
              keys: List[SourceDbActiveObjectDetail],
              current_time: Timestamp): DataFrame = {


    val keysAsCsv: String = getKeysAsCsv(keys)
    val hana_count_query = s""" select count(*) from \"${opts.src_namespace}\".\"${opts.src_dbo_name}\" """

    val sql = hana_count_query
    val df = spark
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map("query" -> (sql)))
      .load()

    val view_count = df.head().getLong(0)

    val sample_percentage =opts.sampling_percentage.toFloat

    val top_records = (view_count * (sample_percentage / 100f)).toLong


    val hana_sampling_query = s""" select top ${top_records} * from \"${opts.src_namespace}\".\"${opts.src_dbo_name}\" """

    val hana_sample_sql = hana_sampling_query
    val result_df = spark
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map("query" -> (hana_sample_sql)))
      .load()


    result_df

  }

  def inferSchema(spark: SparkSession,
                  df: DataFrame,
                  current_time: Timestamp): DataFrame = {


    val schema: StructType = InferSchema(df.rdd, df.schema.fieldNames, df.schema.fields)
    val final_df = spark.sqlContext.createDataFrame(df.rdd, schema)
    final_df

  }

  def getKeysAsCsv(dbaoDetails: List[SourceDbActiveObjectDetail]): String = {

    val keyInArray: Array[AnyRef] = new Array[AnyRef](dbaoDetails.length)

    for ((key: SourceDbActiveObjectDetail, index) <- dbaoDetails.zipWithIndex) {
      keyInArray(index) = "\"" + key.columnName + "\""
    }

    val joiner = Joiner.on(", ").skipNulls()
    joiner.join(keyInArray)

  }

}

