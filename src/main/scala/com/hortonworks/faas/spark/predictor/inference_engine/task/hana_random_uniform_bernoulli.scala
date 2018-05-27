package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp

import com.google.common.base.Joiner
import com.hortonworks.faas.spark.connector.util.InferSchema
import com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngineOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object hana_random_uniform_bernoulli extends Logging {


  log
  val TASK: String = "hana_random_uniform_bernoulli"

  /*

  SQL Select statements returns randomly selected rows forming approximately 10% of the source object
  use Bernoulli sampling in case you can pay the performance cost.

  select * from Products TABLESAMPLE BERNOULLI(10) order by rand()
    or
  select * from Products TABLESAMPLE SYSTEM (10) order by rand();
   */


  def getData(spark: SparkSession,
              opts: InferenceEngineOptions,
              dbaoDetails: List[SourceDbActiveObjectDetail],
              keys: List[SourceDbActiveObjectDetail],
              current_time: Timestamp): DataFrame = {


    val keysAsCsv: String = getKeysAsCsv(keys)
    val hana_sampling_query: String = s"select * from '${opts.src_dbo_name}' TABLESAMPLE BERNOULLI(${opts.sampling_percentage}) order by rand()"

    val sql = hana_sampling_query
    val df = spark
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map("query" -> (sql)))
      .load()

    df

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
      keyInArray(index) = key.sourceColumnName
    }

    val joiner = Joiner.on(", ").skipNulls()
    joiner.join(keyInArray)

  }

}

