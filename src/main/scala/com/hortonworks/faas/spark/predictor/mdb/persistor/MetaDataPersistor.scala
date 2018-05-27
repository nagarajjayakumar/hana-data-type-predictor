package com.hortonworks.faas.spark.predictor.mdb.persistor

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.model.HanaActiveObject
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}

object MetaDataPersistor extends Logging{

  log

  def persist[T](spark: SparkSession, mdpo: MetaDataBaseOptions, ds: Dataset[T]): Unit = {
    mdpo.analytic_type.toLowerCase match {
      case "hana" =>
        import spark.implicits._
        val hmp: hana_metadata_persistor = hana_metadata_persistor(spark, mdpo, ds.as[HanaActiveObject])
        hmp.persist
      case _ =>
        nop_metadata_persistor.persist(spark, mdpo, ds)

    }
  }

  def updateDbActiveObjectDetails(spark: SparkSession,
                                  mdpo: MetaDataBaseOptions,
                                  saod: List[SourceDbActiveObjectDetail],
                                  schemaMap: Map[String, StructType]): Unit = {
    mdpo.analytic_type.toLowerCase match {
      case "hana" =>
        val hmdp: hana_metadata_details_persistor = hana_metadata_details_persistor(spark, mdpo, saod,schemaMap)
        hmdp.updateDbActiveObjectDetails
      case _ =>
        nop_metadata_details_persistor.updateDbActiveObjectDetails(spark, mdpo, saod,schemaMap)

    }
  }


}
