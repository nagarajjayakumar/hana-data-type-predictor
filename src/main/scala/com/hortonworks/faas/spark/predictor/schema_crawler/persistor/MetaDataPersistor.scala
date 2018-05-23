package com.hortonworks.faas.spark.predictor.schema_crawler.persistor

import com.hortonworks.faas.spark.predictor.schema_crawler.model.HanaActiveObject
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MetaDataPersistor {

  def persist(df: DataFrame,spark: SparkSession,  mdpo: MetaDataPersistorOptions): Unit = {
    persist(df,spark, mdpo.analytic_type)
  }


  def persist(df: DataFrame, spark: SparkSession, analytic_type: String): Unit = {
    analytic_type.toLowerCase match {
      case "hana" =>
        import spark.implicits._
        hana_metadata_persistor.persist(df.as[HanaActiveObject])
      case _ =>
        nop_metadata_persistor.persist(df)

    }
  }


}
