package com.hortonworks.faas.spark.predictor.mdb.fetcher

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import org.apache.spark.sql.SparkSession


object MetaDataFetcher {


  def fetchDbActiveObjectDetailsByName(spark: SparkSession,
                                       mdpo: MetaDataBaseOptions): List[SourceDbActiveObjectDetail] = {


    mdpo.analytic_type.toLowerCase match {
      case "hana" =>
        val hmp: hana_metadata_fetcher = hana_metadata_fetcher(spark, mdpo)
        hmp.fetchDbActiveObjectDetailsByName()
      case _ =>
        nop_metadata_fetcher.fetchDbActiveObjectDetailsByName(spark, mdpo)

    }


  }

  def fetchDbActiveObjectDetailsKeysOnly(spark: SparkSession,
                                         mdpo: MetaDataBaseOptions): List[SourceDbActiveObjectDetail] = {


    mdpo.analytic_type.toLowerCase match {
      case "hana" =>
        val hmp: hana_metadata_fetcher = hana_metadata_fetcher(spark, mdpo)
        hmp.fetchDbActiveObjectDetailsKeysOnly()
      case _ =>
        nop_metadata_fetcher.fetchDbActiveObjectDetailsKeysOnly(spark, mdpo)

    }


  }

  def fetchDbActiveObjectDetailsKeysOnly(dboaDetails: List[SourceDbActiveObjectDetail]):
  List[SourceDbActiveObjectDetail] = {

    val filteredDboadetail: List[SourceDbActiveObjectDetail] = dboaDetails.filter {
      dboaDetail => dboaDetail.isKey == true
    }
    filteredDboadetail
  }

}
