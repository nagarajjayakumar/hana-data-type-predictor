package com.hortonworks.faas.spark.predictor.mdb.fetcher

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import org.apache.spark.sql.SparkSession


object MetaDataFetcher {


  def fetchDbActiveObjectDetailByName(spark: SparkSession,
            mdpo: MetaDataBaseOptions,
            namespace: String,
            packge_id: String,
            dboname: String): List[SourceDbActiveObjectDetail] = {


    mdpo.analytic_type.toLowerCase match {
      case "hana" =>
        val hmp: hana_metadata_fetcher = hana_metadata_fetcher(spark, mdpo, namespace, packge_id, dboname)
        hmp.fetchDbActiveObjectDetailByName()
      case _ =>
        nop_metadata_fetcher.fetchDbActiveObjectDetailByName(spark, mdpo, namespace, packge_id, dboname)

    }


  }

  def fetchDbActiveObjectKeysOnly(spark: SparkSession,
            mdpo: MetaDataBaseOptions,
            namespace: String,
            packge_id: String,
            dboname: String): List[SourceDbActiveObjectDetail] = {


    mdpo.analytic_type.toLowerCase match {
      case "hana" =>
        val hmp: hana_metadata_fetcher = hana_metadata_fetcher(spark, mdpo, namespace, packge_id, dboname)
        hmp.fetchDbActiveObjectKeysOnly()
      case _ =>
        nop_metadata_fetcher.fetchDbActiveObjectKeysOnly(spark, mdpo, namespace, packge_id, dboname)

    }


  }

}
