package com.hortonworks.faas.spark.predictor.schema_crawler.persistor

import com.hortonworks.faas.spark.predictor.inference_engine.configuration.ConfigurationOptionMap
import com.hortonworks.faas.spark.predictor.util.Logging


class MetaDataPersistorOptions(val analytic_type: String) {}

/**
  * Created by njayakumar on 5/16/2018.
  */
object MetaDataPersistorOptions extends Logging {

  val ANALYTIC_TYPE = "analytic_type"

  def apply(options: ConfigurationOptionMap): MetaDataPersistorOptions = {
    val out: String = if (options.opts.contains(ANALYTIC_TYPE) && options.opts(ANALYTIC_TYPE).nonEmpty) options.opts(ANALYTIC_TYPE).head else ""
    new MetaDataPersistorOptions(out)
  }

  def printUsage(): Unit = {
    logInfo(s"--${ANALYTIC_TYPE} | Analytic Type [Select between HANA | ORACLE | MSSQL | MYSQL | OTHERS]")

  }
}
