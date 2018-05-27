package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp

import com.google.common.base.Joiner
import com.hortonworks.faas.spark.connector.util.InferSchema
import com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngineOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object hana_startified_dynamic_population extends Logging {

  log

  val TASK: String = "hana_startified_dynamic_population"

  /*

   Calculate the population size for each group "on the fly":
    select d.*
    from (select d.*,
                 row_number() over (partition by coursecode order by newid) as seqnum,
                 count(*) over () as cnt,
                 count(*) over (partition by coursecode) as cc_cnt
          from degree d
         ) d
    where seqnum < 500 * (cc_cnt * 1.0 / cnt)
    */


  def getData(spark: SparkSession,
              opts: InferenceEngineOptions,
              dbaoDetails: List[SourceDbActiveObjectDetail],
              keys: List[SourceDbActiveObjectDetail],
              current_time: Timestamp): DataFrame = {


    val keysAsCsv: String = getKeysAsCsv(keys)
    val hana_sampling_query: String = s"select * from (" +
      s"                                     select *, ROW_NUMBER() OVER (PARTITION BY ${keysAsCsv} ORDER BY rnd) as rnk," +
      s"                                     count(*) over () as cnt, count(*) over (partition by ${keysAsCsv}) as cc_cnt" +
      s"                                     FROM ( " +
      s"                                            SELECT *, RAND() AS rnd  FROM  '${opts.src_dbo_name}'   " +
      s"                                           ) bucketed" +
      s"                                              ) sampled where rnk < ${opts.sampling_size} * (cc_cnt * 1.0 / cnt) "

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
