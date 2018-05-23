package com.hortonworks.faas.spark.predictor.schema_crawler.persistor

import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.DataFrame

object nop_metadata_persistor extends Logging {

  def persist(df: DataFrame): Unit = {
    logError("FATAL :: No Persistor for the given advanced analytic type ... ")
  }

}
