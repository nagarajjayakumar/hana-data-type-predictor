package com.hortonworks.faas.spark.predictor.inference_engine.analytic.common.analytic

/**
  * Created by njayakumar on 5/16/2018.
  */
object AdvancedAnalyticType extends Enumeration {
  val HANADB = Value("HANADB")
  val ORACLE = Value("ORACLE")
  val MSSQL = Value("MSSQL")
  val MYSQL = Value("MYSQL")
  val Unknown = Value("Unknown")

  def withNameWithDefault(name: String): Value =
    values.find(_.toString.toLowerCase == name.toLowerCase()).getOrElse(Unknown)

}
