package com.hortonworks.faas.spark.predictor.mdb.persistor

import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.{Dataset, SparkSession}

object nop_metadata_persistor extends Logging {

  def persist[T](ds: Dataset[T], spark: SparkSession, mdbenvironment: String, mdbservice: String): Unit = {
    logError("FATAL :: No Persistor for the given advanced analytic type ... ")
  }

}
