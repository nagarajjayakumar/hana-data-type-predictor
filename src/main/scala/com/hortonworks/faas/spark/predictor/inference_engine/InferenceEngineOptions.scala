package com.hortonworks.faas.spark.predictor.inference_engine

import com.hortonworks.faas.spark.predictor.inference_engine.analytic.common._
import com.hortonworks.faas.spark.predictor.inference_engine.configuration.ConfigurationOptionMap
import com.hortonworks.faas.spark.predictor.inference_engine.task.inference_engine_master

/**
  * Created by njayakumar on 5/16/2018.
  */
class InferenceEngineOptions(val task: String,
                             val analytic_type: String,
                             val src_dbo_name: String,
                             val o: String,
                             val w: String) {

  def this(t: String, at: String, src_dbo_name: String, cdfw: CommonDataFrameWriterOption) {
    this(t, at, src_dbo_name, cdfw.output, cdfw.write_mode)
  }

  def isValid(): Boolean = {
    true
  }
}

object InferenceEngineOptions {
  val TASK_KEY = "task"

  val ANALYTIC_TYPE = "analytic_type"

  val SRCDBONAME = "src_dbo_name"

  def apply(args: Array[String]): InferenceEngineOptions = {
    apply(ConfigurationOptionMap(args))
  }

  def apply(options: ConfigurationOptionMap): InferenceEngineOptions = {

    val cdfw: CommonDataFrameWriterOption = CommonDataFrameWriterOption(options)

    val t: String = if (options.opts.contains(TASK_KEY) && options.opts(TASK_KEY).nonEmpty) options.opts(TASK_KEY)(0) else inference_engine_master.TASK

    val at = if (options.opts.contains(ANALYTIC_TYPE) && options.opts(ANALYTIC_TYPE).nonEmpty) options.opts(ANALYTIC_TYPE)(0) else inference_engine_master.ANALYTIC_TYPE.toString

    val src_dbo_name = if (options.opts.contains(SRCDBONAME) && options.opts(SRCDBONAME).nonEmpty) options.opts(SRCDBONAME)(0) else inference_engine_master.SRCDBONAME

    new InferenceEngineOptions(t, at, src_dbo_name, cdfw)
  }

  def printUsage(): Unit = {
    CommonDataFrameWriterOption.printUsage()

    println(s"${TASK_KEY} | Task to perform : { ${inference_engine_master.TASK}, ${inference_engine_master.TASK}, ${inference_engine_master.TASK} }; Default: ${inference_engine_master.TASK}")
  }
}
