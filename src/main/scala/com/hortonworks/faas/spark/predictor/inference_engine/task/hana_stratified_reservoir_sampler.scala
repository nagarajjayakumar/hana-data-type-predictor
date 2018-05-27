package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp

import com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngineOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.google.common.base.Joiner
import com.hortonworks.faas.spark.connector.util.InferSchema
import org.apache.spark.sql.types.StructType

object hana_stratified_reservoir_sampler {


  val TASK: String = "hana_stratified_reservoir_sampler"

  /*
  SELECT MANDT, SPRAS, GNTYP, GNTXT
FROM
(
    SELECT *, ROW_NUMBER() OVER (PARTITION BY MANDT ORDER BY rnd) as rnk
    FROM (
        SELECT MANDT, SPRAS,GNTYP,GNTXT, RAND() AS rnd
        FROM  SLTECC.T352T_T
    ) bucketed
) sampled
-- assuming we want 3 records from each group
WHERE rnk <= 3
   */



  def inferSchema(spark: SparkSession,
                  opts: InferenceEngineOptions,
                  keys : List[SourceDbActiveObjectDetail],
                  current_time: Timestamp): DataFrame = {


    val keysAsCsv: String = getKeysAsCsv(keys)
    val hana_active_object_query: String = s"select * from (" +
      s"                                     select *, ROW_NUMBER() OVER (PARTITION BY ${keysAsCsv} ORDER BY rnd) as rnk FROM ( " +
      s"                                            SELECT *, RAND() AS rnd  FROM  '${opts.src_dbo_name}'   ) bucketed" +
      s"                                     ) sampled where rnk <= ${opts.sampling_size}  "

    val sql = hana_active_object_query
    val df = spark
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map("query" -> (sql)))
      .load()

    val schema: StructType = InferSchema(df.rdd,df.schema.fieldNames, df.schema.fields)

    df

  }

  def getKeysAsCsv(keys : List[SourceDbActiveObjectDetail]): String ={

    val keyInArray : Array[AnyRef] = new Array[AnyRef](keys.length)

    for((key: SourceDbActiveObjectDetail, index) <- keys.zipWithIndex){
      keyInArray(index) = key.sourceColumnName
    }

    val joiner = Joiner.on(", ").skipNulls()
    joiner.join(keyInArray)

  }


}
