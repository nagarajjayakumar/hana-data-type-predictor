package com.hortonworks.faas.spark.predictor.mdb.persistor

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.model.HanaActiveObject
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object MetaDataPersistor {


  def persist[T]( spark: SparkSession,  mdpo: MetaDataBaseOptions, ds: Dataset[T]): Unit = {
    mdpo.analytic_type.toLowerCase match {
      case "hana" =>
        import spark.implicits._
        val hmp : hana_metadata_persistor = hana_metadata_persistor( spark, mdpo, ds.as[HanaActiveObject])
        hmp.persist
      case _ =>
        nop_metadata_persistor.persist( spark, mdpo, ds)

    }
  }


}
