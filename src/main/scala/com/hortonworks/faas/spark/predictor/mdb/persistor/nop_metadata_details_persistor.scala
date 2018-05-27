package com.hortonworks.faas.spark.predictor.mdb.persistor

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object nop_metadata_details_persistor extends Logging {

  log

  def updateDbActiveObjectDetails(spark: SparkSession,
                                  mdpopts: MetaDataBaseOptions,
                                  saod: List[SourceDbActiveObjectDetail],
                                  schemaMap: Map[String, StructType]): Unit = {

    logError("FATAL :: No Persistor for the given advanced analytic type ... ")

  }
}
