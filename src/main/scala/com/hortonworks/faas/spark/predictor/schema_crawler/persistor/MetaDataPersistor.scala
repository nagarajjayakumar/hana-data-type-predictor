package com.hortonworks.faas.spark.predictor.schema_crawler.persistor

import com.hortonworks.faas.spark.predictor.schema_crawler.model.HanaActiveObject
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object MetaDataPersistor {

  def persist[T](ds: Dataset[T], spark: SparkSession, mdpo: MetaDataPersistorOptions): Unit = {
    persist(ds, spark, mdpo.analytic_type, mdpo.mdbenvironment, mdpo.mdbservice)
  }


  def persist[T](ds: Dataset[T], spark: SparkSession, analytic_type: String, mdbenvironment: String, mdbservice: String): Unit = {
    analytic_type.toLowerCase match {
      case "hana" =>
        import spark.implicits._
        val hmp : hana_metadata_persistor = hana_metadata_persistor(ds.as[HanaActiveObject], spark, mdbenvironment, mdbservice)
        hmp.persist
      case _ =>
        nop_metadata_persistor.persist(ds, spark, mdbenvironment, mdbservice)

    }
  }


}
