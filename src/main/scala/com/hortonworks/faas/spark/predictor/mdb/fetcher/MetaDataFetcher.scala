package com.hortonworks.faas.spark.predictor.mdb.fetcher

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import org.apache.spark.sql.SparkSession


object MetaDataFetcher {


  def fetch(spark: SparkSession,
            mdpo: MetaDataBaseOptions,
            namespace: String,
            packge_id: String,
            dboname: String): Unit = {


    mdpo.analytic_type.toLowerCase match {
      case "hana" =>
        val hmp: hana_metadata_fetcher = hana_metadata_fetcher(spark, mdpo, namespace, packge_id, dboname)
        hmp.fetchByName()
      case _ =>
        nop_metadata_fetcher.fetchByName(spark, mdpo, namespace, packge_id, dboname)

    }


  }

}
