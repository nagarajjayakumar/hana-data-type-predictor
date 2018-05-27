package com.hortonworks.faas.spark.predictor.mdb.fetcher

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import org.apache.spark.sql.SparkSession


object MetaDataFetcher {


  def fetchDbActiveObjectDetailByName(spark: SparkSession,
            mdpo: MetaDataBaseOptions ): List[SourceDbActiveObjectDetail] = {


    mdpo.analytic_type.toLowerCase match {
      case "hana" =>
        val hmp: hana_metadata_fetcher = hana_metadata_fetcher(spark, mdpo)
        hmp.fetchDbActiveObjectDetailByName()
      case _ =>
        nop_metadata_fetcher.fetchDbActiveObjectDetailByName(spark, mdpo)

    }


  }

  def fetchDbActiveObjectKeysOnly(spark: SparkSession,
            mdpo: MetaDataBaseOptions ): List[SourceDbActiveObjectDetail] = {


    mdpo.analytic_type.toLowerCase match {
      case "hana" =>
        val hmp: hana_metadata_fetcher = hana_metadata_fetcher(spark, mdpo)
        hmp.fetchDbActiveObjectKeysOnly()
      case _ =>
        nop_metadata_fetcher.fetchDbActiveObjectKeysOnly(spark, mdpo)

    }


  }

}
