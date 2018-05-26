package com.hortonworks.faas.spark.predictor.inference_engine

import com.hortonworks.faas.spark.predictor.configuration.ConfigurationOptionMap
import com.hortonworks.faas.spark.predictor.inference_engine.analytic.common._
import com.hortonworks.faas.spark.predictor.inference_engine.task.inference_engine_master

/**
  * Created by njayakumar on 5/16/2018.
  */
class InferenceEngineOptions(val task: String,
                             val analytic_type: String,
                             val src_dbo_name: String,
                             val sampling_techniq: String,
                             val sampling_percentage: String,
                             val runtime_env: String,
                             val src_namespace: String,
                             val o: String,
                             val w: String) {

  def this(t: String, at: String, src_dbo_name: String, sampling_techniq:
           String, sampling_percentage: String, runtime_env: String, src_namespace: String,
           cdfw: CommonDataFrameWriterOption) {
    this(t, at, src_dbo_name,sampling_techniq, sampling_percentage, runtime_env, src_namespace, cdfw.output, cdfw.write_mode)
  }

  def isValid(): Boolean = {
    true
  }
}

object InferenceEngineOptions {
  val TASK_KEY = "task"

  val ANALYTIC_TYPE = "analytic_type"

  val SRCDBONAME = "src_dbo_name"

  val SAMPLING_TECHNIQ = "full"

  val SAMPLING_PERCENTAGE = "1"

  val RUNTIME_ENV = "local"

  val SRCNAMESPACE = "dbName"

  def apply(args: Array[String]): InferenceEngineOptions = {
    apply(ConfigurationOptionMap(args))
  }

  def apply(options: ConfigurationOptionMap): InferenceEngineOptions = {

    val cdfw: CommonDataFrameWriterOption = CommonDataFrameWriterOption(options)

    val t: String = if (options.opts.contains(TASK_KEY) && options.opts(TASK_KEY).nonEmpty) options.opts(TASK_KEY)(0) else inference_engine_master.TASK

    val at = if (options.opts.contains(ANALYTIC_TYPE) && options.opts(ANALYTIC_TYPE).nonEmpty) options.opts(ANALYTIC_TYPE)(0) else inference_engine_master.ANALYTIC_TYPE.toString

    val src_dbo_name = if (options.opts.contains(SRCDBONAME) && options.opts(SRCDBONAME).nonEmpty) options.opts(SRCDBONAME)(0) else inference_engine_master.SRCDBONAME

    val sampling_techniq = if (options.opts.contains(SAMPLING_TECHNIQ) && options.opts(SAMPLING_TECHNIQ).nonEmpty) options.opts(SAMPLING_TECHNIQ)(0) else inference_engine_master.SAMPLING_TECHNIQ

    val sampling_percentage = if (options.opts.contains(SAMPLING_PERCENTAGE) && options.opts(SAMPLING_PERCENTAGE).nonEmpty) options.opts(SAMPLING_PERCENTAGE)(0) else inference_engine_master.SAMPLING_PERCENTAGE

    val runtime_env = if (options.opts.contains(RUNTIME_ENV) && options.opts(RUNTIME_ENV).nonEmpty) options.opts(RUNTIME_ENV)(0) else inference_engine_master.RUNTIME_ENV

    val src_namespace = if (options.opts.contains(SRCNAMESPACE) && options.opts(SRCNAMESPACE).nonEmpty) options.opts(SRCNAMESPACE)(0) else inference_engine_master.SRCNAMESPACE

    new InferenceEngineOptions(t, at, src_dbo_name,sampling_techniq, sampling_percentage,runtime_env,src_namespace, cdfw)
  }

  def printUsage(): Unit = {
    CommonDataFrameWriterOption.printUsage()

    println(s"${TASK_KEY} | Task to perform : { ${inference_engine_master.TASK}, ${inference_engine_master.TASK}, ${inference_engine_master.TASK} }; Default: ${inference_engine_master.TASK}")
  }
}
