package com.hortonworks.faas.spark.predictor.mdb.common

import com.hortonworks.faas.spark.predictor.configuration.ConfigurationOptionMap
import com.hortonworks.faas.spark.predictor.util.Logging


class MetaDataBaseOptions(val analytic_type: String,
                          val mdbenvironment: String,
                          val mdbservice: String,
                          val src_namespace: String,
                          val src_dbo_name: String) {

  def isValid(): Boolean = {
    true
  }
}

/**
  * Created by njayakumar on 5/16/2018.
  */
object MetaDataBaseOptions extends Logging {

  val ANALYTIC_TYPE = "analytic_type"
  val MDBENVIRONMENT = "mdbenv"
  val MDBSERVICE = "mdbservice"
  val SRCDBONAME = "src_dbo_name"
  val SRCNAMESPACE = "dbName"


  def apply(args: Array[String]): MetaDataBaseOptions = {
    apply(ConfigurationOptionMap(args))
  }

  def apply(options: ConfigurationOptionMap): MetaDataBaseOptions = {

    val analytic_type: String = if (options.opts.contains(ANALYTIC_TYPE) && options.opts(ANALYTIC_TYPE).nonEmpty) options.opts(ANALYTIC_TYPE).head else "unknown"
    val mdbenvironment: String = if (options.opts.contains(MDBENVIRONMENT) && options.opts(MDBENVIRONMENT).nonEmpty) options.opts(MDBENVIRONMENT).head else "development_mysql"
    val mdbservice: String = if (options.opts.contains(MDBSERVICE) && options.opts(MDBSERVICE).nonEmpty) options.opts(MDBSERVICE).head else "service"

    val src_namespace = if (options.opts.contains(SRCNAMESPACE) && options.opts(SRCNAMESPACE).nonEmpty) options.opts(SRCNAMESPACE)(0) else "default"
    val src_dbo_name = if (options.opts.contains(SRCDBONAME) && options.opts(SRCDBONAME).nonEmpty) options.opts(SRCDBONAME)(0) else "dual"

    new MetaDataBaseOptions(analytic_type, mdbenvironment, mdbservice, src_namespace, src_dbo_name)
  }

  def printUsage(): Unit = {
    logInfo(s"--${ANALYTIC_TYPE} | Analytic Type    [Select between HANA | ORACLE | MSSQL | MYSQL | OTHERS]")
    logInfo(s"--${MDBENVIRONMENT}   | Environment Type [Select between DEV | INT | PROD | OTHERS]")
    logInfo(s"--${MDBSERVICE}     | DB Service Type  [Select between service | others]")
    logInfo(s"--${SRCNAMESPACE}   | Source DB or Schema or Namespace   ")
    logInfo(s"--${SRCDBONAME}     | Source Database Object Name  ")
  }
}
