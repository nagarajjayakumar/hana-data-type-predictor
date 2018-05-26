package com.hortonworks.faas.spark.predictor.inference_engine.analytic.common

import com.hortonworks.faas.spark.predictor.configuration.ConfigurationOptionMap
import com.hortonworks.faas.spark.predictor.util.Logging

class CommonDataFrameWriterOption(val output: String, val write_mode: String) {}

/**
  * Created by njayakumar on 5/16/2018.
  */
object CommonDataFrameWriterOption extends Logging {
  val OUTPUT_KEY: String = "output"
  val WRITE_MODE_KEY: String = "write_mode"

  def apply(options: ConfigurationOptionMap): CommonDataFrameWriterOption = {
    val out: String = if (options.opts.contains(OUTPUT_KEY) && options.opts(OUTPUT_KEY).nonEmpty) options.opts(OUTPUT_KEY).head else ""
    val mode: String = if (options.opts.contains(WRITE_MODE_KEY) && options.opts(WRITE_MODE_KEY).nonEmpty) options.opts(WRITE_MODE_KEY).head else ""
    new CommonDataFrameWriterOption(out, mode)
  }

  def printUsage(): Unit = {
    logInfo(s"--${OUTPUT_KEY} | Output data location")
    logInfo(s"--${WRITE_MODE_KEY} | Select between CSV, ORC or JSON output formats")
  }
}
