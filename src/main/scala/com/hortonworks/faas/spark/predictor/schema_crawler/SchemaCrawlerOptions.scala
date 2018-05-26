package com.hortonworks.faas.spark.predictor.schema_crawler

import com.hortonworks.faas.spark.predictor.configuration.ConfigurationOptionMap
import com.hortonworks.faas.spark.predictor.inference_engine.analytic.common._
import com.hortonworks.faas.spark.predictor.schema_crawler.task.schema_crawler_master

/**
  * Created by njayakumar on 5/16/2018.
  */
class SchemaCrawlerOptions(val task: String,
                           val analytic_type: String,
                           val src_dbo_name: String,
                           val runtime_env: String,
                           val src_namespace: String,
                           val o: String,
                           val w: String) extends CommonDataFrameWriterOption(o, w) {

  def this(t: String, at: String, src_dbo_name: String, runtime_env: String, src_namespace: String, cdfw: CommonDataFrameWriterOption) {
    this(t, at, src_dbo_name,runtime_env, src_namespace, cdfw.output, cdfw.write_mode)
  }

  def isValid(): Boolean = {
    true
  }
}

object SchemaCrawlerOptions {
  val TASK_KEY = "task"

  val ANALYTIC_TYPE = "analytic_type"

  val SRCDBONAME = "src_dbo_name"

  val RUNTIME_ENV = "runtime_env"

  val SRCNAMESPACE = "src_name_space"

  def apply(args: Array[String]): SchemaCrawlerOptions = {
    apply(ConfigurationOptionMap(args))
  }

  def apply(options: ConfigurationOptionMap): SchemaCrawlerOptions = {

    val cdfw: CommonDataFrameWriterOption = CommonDataFrameWriterOption(options)

    val t: String = if (options.opts.contains(TASK_KEY) && options.opts(TASK_KEY).nonEmpty) options.opts(TASK_KEY)(0) else schema_crawler_master.TASK

    val at = if (options.opts.contains(ANALYTIC_TYPE) && options.opts(ANALYTIC_TYPE).nonEmpty) options.opts(ANALYTIC_TYPE)(0) else schema_crawler_master.ANALYTIC_TYPE.toString

    val src_dbo_name = if (options.opts.contains(SRCDBONAME) && options.opts(SRCDBONAME).nonEmpty) options.opts(SRCDBONAME)(0) else schema_crawler_master.SRCDBONAME

    val runtime_env = if (options.opts.contains(RUNTIME_ENV) && options.opts(RUNTIME_ENV).nonEmpty) options.opts(RUNTIME_ENV)(0) else schema_crawler_master.RUNTIME_ENV

    val src_namespace = if (options.opts.contains(SRCNAMESPACE) && options.opts(SRCNAMESPACE).nonEmpty) options.opts(SRCNAMESPACE)(0) else schema_crawler_master.SRCNAMESPACE

    new SchemaCrawlerOptions(t, at, src_dbo_name, runtime_env,src_namespace, cdfw)
  }

  def printUsage(): Unit = {
    CommonDataFrameWriterOption.printUsage()

    println(s"${TASK_KEY} | Task to perform : { ${schema_crawler_master.TASK}, ${schema_crawler_master.TASK}, ${schema_crawler_master.TASK} }; Default: ${schema_crawler_master.TASK}")
  }
}
