package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp

import com.google.common.base.Joiner
import com.hortonworks.faas.spark.connector.util.InferSchema
import com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngineOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object hana_stratified_constant_proportion extends Logging{


  log
  val TASK: String = "hana_stratified_constant_proportion"

  /*

  if we assume or confirm that there are more than 100 records in each group,
  then we can select the X% records from each group in a single pass using NTILE function as shown below.

      SELECT MANDT, SPRAS, GNTYP, GNTXT
    FROM
    (
        SELECT  *, NTILE(100) OVER (PARTITION BY MANDT ORDER BY rnd) as tile
        FROM (
            SELECT MANDT, SPRAS,GNTYP,GNTXT, RAND() AS rnd
            FROM  SLTECC.T352T_T
        ) bucketed
    ) sampled
    -- assuming we want 10% records from each letter group
    WHERE tile <= 10
   */


  def getData(spark: SparkSession,
              opts: InferenceEngineOptions,
              dbaoDetails : List[SourceDbActiveObjectDetail],
              keys: List[SourceDbActiveObjectDetail],
              current_time: Timestamp): DataFrame = {


    val keysAsCsv: String = getKeysAsCsv(keys)
    val hana_sampling_query: String = s"select * from (" +
      s"                                     SELECT  *, NTILE(${opts.sampling_size}) OVER (PARTITION BY ${keysAsCsv} ORDER BY rnd) as tile FROM ( " +
      s"                                            SELECT *, RAND() AS rnd  FROM  '${opts.src_dbo_name}'   ) bucketed" +
      s"                                     ) sampled where tile <= ${opts.sampling_percentage}  "

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
      keyInArray(index) = key.sourceColumnName
    }

    val joiner = Joiner.on(", ").skipNulls()
    joiner.join(keyInArray)

  }

}
