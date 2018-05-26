package com.hortonworks.faas.spark.predictor.mdb.persistor

import com.hortonworks.faas.spark.predictor.configuration.ConfigurationOptionMap
import com.hortonworks.faas.spark.predictor.util.Logging


class MetaDataPersistorOptions(val analytic_type: String, val mdbenvironment: String, val mdbservice: String) {

  def isValid(): Boolean = {
    true
  }
}

/**
  * Created by njayakumar on 5/16/2018.
  */
object MetaDataPersistorOptions extends Logging {

  val ANALYTIC_TYPE = "analytic_type"
  val MDBENVIRONMENT = "mdbenv"
  val MDBSERVICE = "mdbservice"


  def apply(args: Array[String]): MetaDataPersistorOptions = {
    apply(ConfigurationOptionMap(args))
  }

  def apply(options: ConfigurationOptionMap): MetaDataPersistorOptions = {
    val analytic_type: String = if (options.opts.contains(ANALYTIC_TYPE) && options.opts(ANALYTIC_TYPE).nonEmpty) options.opts(ANALYTIC_TYPE).head else "unknown"
    val mdbenvironment: String = if (options.opts.contains(MDBENVIRONMENT) && options.opts(MDBENVIRONMENT).nonEmpty) options.opts(MDBENVIRONMENT).head else "development_mysql"
    val mdbservice: String = if (options.opts.contains(MDBSERVICE) && options.opts(MDBSERVICE).nonEmpty) options.opts(MDBSERVICE).head else "service"

    new MetaDataPersistorOptions(analytic_type, mdbenvironment, mdbservice)
  }

  def printUsage(): Unit = {
    logInfo(s"--${ANALYTIC_TYPE} | Analytic Type    [Select between HANA | ORACLE | MSSQL | MYSQL | OTHERS]")
    logInfo(s"--${MDBENVIRONMENT}   | Environment Type [Select between DEV | INT | PROD | OTHERS]")
    logInfo(s"--${MDBSERVICE}     | DB Service Type  [Select between service | others]")
  }
}
