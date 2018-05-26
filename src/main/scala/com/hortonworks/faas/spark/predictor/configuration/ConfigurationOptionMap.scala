package com.hortonworks.faas.spark.predictor.configuration

import scala.collection.immutable.HashMap

/**
  * Created by njayakumar on 5/16/2018.
  */
class ConfigurationOptionMap(val opts: Map[String, Seq[String]])

object ArgumentType extends Enumeration {
  type ArgumentType = Value
  val Unknown, Flag, Key, ValueArg = Value
}

object ConfigurationOptionMap {

  import ArgumentType._

  /**
    * Key list order is not maintained
    * Value list order is maintained
    *
    * @param args
    * @return
    */
  def apply(args: Array[String]): ConfigurationOptionMap = {
    var opts: Map[String, Seq[String]] = HashMap.empty[String, Seq[String]]

    var prevKey = ""

    for (i <- args) {
      classifyArgument(i) match {
        case (ValueArg, v: String) =>
          opts += ( prevKey -> (opts(prevKey) :+ v))
        case (Key, k: String) =>
          if (!opts.contains(k)) {
            opts += (k -> Seq[String]())
          }
          prevKey = k
        case (Flag, v: String) =>
          if (!opts.contains(v)) {
            opts += (v -> null)
          }
        case (Unknown, v: String) =>
      }
    }
    new ConfigurationOptionMap( opts )
  }

  def classifyArgument( arg: String ): (ArgumentType, String) = {
    if (arg.isEmpty)
      (Unknown, arg)
    else if (arg.startsWith("--"))
      (Key, arg.substring(2))
    else if (arg.startsWith("-"))
      (Flag, arg.substring(1))
    else
      (ValueArg, arg)
  }
}