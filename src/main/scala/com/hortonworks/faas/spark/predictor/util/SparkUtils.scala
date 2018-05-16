package com.hortonworks.faas.spark.predictor.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkUtils {

  /**
    * Given the name, arguments array and argument number for the spark master, return
    * a spark builder that optionally sets the master.
    */
  def createSparkBuilder(name:String, conf: SparkConf, args: Array[String], argNumber: Int): SparkSession.Builder = {
    val sparkBuilder = SparkSession
      .builder()
      .config(conf)
      .appName(name)
      .enableHiveSupport()
    if (args.length > argNumber) {
      sparkBuilder.master(args(argNumber))
    } else {
      sparkBuilder
    }
  }
}
