package com.hortonworks.faas.spark.predictor.schema_crawler

import com.hortonworks.faas.spark.predictor.inference_engine.analytic.common._
import com.hortonworks.faas.spark.predictor.inference_engine.configuration.ConfigurationOptionMap
import com.hortonworks.faas.spark.predictor.schema_crawler.task.schema_crawler_master

/**
  * Created by njayakumar on 5/16/2018.
  */
class SchemaCrawlerOptions(val task: String, val o: String, val w: String) extends CommonDataFrameWriterOption(o, w) {
  def this(t: String, cdfw: CommonDataFrameWriterOption) {
    this(t, cdfw.output, cdfw.write_mode)
  }

  def isValid(): Boolean = {
    true
  }
}

object SchemaCrawlerOptions {
  val TASK_KEY = "task"

  def apply(args: Array[String]): SchemaCrawlerOptions = {
    apply(ConfigurationOptionMap(args))
  }

  def apply(options: ConfigurationOptionMap): SchemaCrawlerOptions = {

    val cdfw: CommonDataFrameWriterOption = CommonDataFrameWriterOption(options)

    val t: String = if (options.opts.contains(TASK_KEY) && options.opts(TASK_KEY).nonEmpty) options.opts(TASK_KEY)(0) else schema_crawler_master.TASK

    new SchemaCrawlerOptions(t, cdfw)
  }

  def printUsage(): Unit = {
    CommonDataFrameWriterOption.printUsage()

    println(s"${TASK_KEY} | Task to perform : { ${schema_crawler_master.TASK}, ${schema_crawler_master.TASK}, ${schema_crawler_master.TASK} }; Default: ${schema_crawler_master.TASK}")
  }
}
