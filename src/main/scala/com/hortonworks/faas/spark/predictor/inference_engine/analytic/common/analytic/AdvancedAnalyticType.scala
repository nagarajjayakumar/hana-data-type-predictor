package com.hortonworks.faas.spark.predictor.inference_engine.analytic.common.analytic

/**
  * Created by njayakumar on 5/16/2018.
  */
object AdvancedAnalyticType extends Enumeration {
  val HANA = Value("HANA")
  val ORACLE = Value("ORACLE")
  val MSSQL = Value("MSSQL")
  val MYSQL = Value("MYSQL")
  val OTHERS = Value("OTHERS")
  val Unknown = Value("Unknown")

  def withNameWithDefault(name: String): Value =
    values.find(_.toString.toLowerCase == name.toLowerCase()).getOrElse(Unknown)

}
