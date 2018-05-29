package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp

import com.google.common.base.Joiner
import com.hortonworks.faas.spark.connector.util.InferSchema
import com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngineOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object hana_no_sampling_techniq extends Logging{

  log

  val TASK: String = "hana_no_sampling_techniq"

  /*
    No sampling techniq - Only use for the relative small tables
   */
  def getData(spark: SparkSession,
                  opts: InferenceEngineOptions,
                  dbaoDetails : List[SourceDbActiveObjectDetail],
                  keys: List[SourceDbActiveObjectDetail],
                  current_time: Timestamp): DataFrame = {


    val keysAsCsv: String = getKeysAsCsv(keys)
    val hana_sampling_query: String = s""" select * from \"${opts.src_namespace}\".\"${opts.src_dbo_name}\"  """

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


    val schema: StructType = InferSchema(df.rdd,df.schema.fieldNames, df.schema.fields)
    val final_df = spark.sqlContext.createDataFrame(df.rdd, schema)
    final_df

  }

  def getKeysAsCsv(dbaoDetails : List[SourceDbActiveObjectDetail]): String ={

    val keyInArray : Array[AnyRef] = new Array[AnyRef](dbaoDetails.length)

    for((key: SourceDbActiveObjectDetail, index) <- dbaoDetails.zipWithIndex){
      keyInArray(index) = "\"" + key.columnName + "\""
    }

    val joiner = Joiner.on(", ").skipNulls()
    joiner.join(keyInArray)

  }

}
