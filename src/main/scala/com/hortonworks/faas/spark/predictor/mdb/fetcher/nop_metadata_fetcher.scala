package com.hortonworks.faas.spark.predictor.mdb.fetcher

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.SparkSession

object nop_metadata_fetcher extends Logging {

  def fetchDbActiveObjectDetailByName(spark: SparkSession,
                  mdpopts: MetaDataBaseOptions ): List[SourceDbActiveObjectDetail] = {
    logError("FATAL :: No data fetcher for the given advanced analytic type ... ")
    List[SourceDbActiveObjectDetail]()
  }

  def fetchDbActiveObjectKeysOnly(spark: SparkSession,
                  mdpopts: MetaDataBaseOptions): List[SourceDbActiveObjectDetail] = {
    logError("FATAL :: No data fetcher for the given advanced analytic type ... ")
    List[SourceDbActiveObjectDetail]()
  }

}