package com.hortonworks.faas.spark.predictor.schema_crawler.persistor

import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.{Dataset, SparkSession}

object nop_metadata_persistor extends Logging {

  def persist[T](ds: Dataset[T], spark: SparkSession, environment: String, dbService: String): Unit = {
    logError("FATAL :: No Persistor for the given advanced analytic type ... ")
  }

}
