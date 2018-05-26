package com.hortonworks.faas.spark.predictor.mdb.persistor

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.{Dataset, SparkSession}

object nop_metadata_persistor extends Logging {

  def persist[T]( spark: SparkSession,mdpopts: MetaDataBaseOptions, ds: Dataset[T]): Unit = {
    logError("FATAL :: No Persistor for the given advanced analytic type ... ")
  }

}
