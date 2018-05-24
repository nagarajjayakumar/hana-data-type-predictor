package com.hortonworks.faas.spark.predictor.schema_crawler.persistor

import com.hortonworks.faas.spark.predictor.inference_engine.configuration.ConfigurationOptionMap
import com.hortonworks.faas.spark.predictor.util.Logging


class MetaDataPersistorOptions(val analytic_type: String , val environment: String, val dbService: String) {}

/**
  * Created by njayakumar on 5/16/2018.
  */
object MetaDataPersistorOptions extends Logging {

  val ANALYTIC_TYPE = "analytic_type"
  val ENVIRONMENT = "env"
  val DBSERVICE = "service"

  def apply(options: ConfigurationOptionMap): MetaDataPersistorOptions = {
    val analytic_type: String = if (options.opts.contains(ANALYTIC_TYPE) && options.opts(ANALYTIC_TYPE).nonEmpty) options.opts(ANALYTIC_TYPE).head else "unknown"
    val environment: String = if (options.opts.contains(ENVIRONMENT) && options.opts(ENVIRONMENT).nonEmpty) options.opts(ENVIRONMENT).head else "development_mysql"
    val dbservice: String = if (options.opts.contains(DBSERVICE) && options.opts(DBSERVICE).nonEmpty) options.opts(DBSERVICE).head else "service"

    new MetaDataPersistorOptions(analytic_type, environment, dbservice)
  }

  def printUsage(): Unit = {
    logInfo(s"--${ANALYTIC_TYPE} | Analytic Type    [Select between HANA | ORACLE | MSSQL | MYSQL | OTHERS]")
    logInfo(s"--${ENVIRONMENT}   | Environment Type [Select between DEV | INT | PROD | OTHERS]")
    logInfo(s"--${DBSERVICE}     | DB Service Type  [Select between service | others]")
  }
}
