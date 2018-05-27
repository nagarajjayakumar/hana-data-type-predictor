package com.hortonworks.faas.spark.predictor.mdb.fetcher

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.SparkSession

object nop_metadata_fetcher extends Logging {

  def fetchByName(spark: SparkSession,
                  mdpopts: MetaDataBaseOptions,
                  namespace: String,
                  packge_id: String,
                  dboname: String ): Unit = {
    logError("FATAL :: No data fetcher for the given advanced analytic type ... ")
  }

}