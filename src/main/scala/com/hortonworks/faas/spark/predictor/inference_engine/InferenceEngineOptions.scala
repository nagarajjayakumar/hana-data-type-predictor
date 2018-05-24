package com.hortonworks.faas.spark.predictor.inference_engine

import com.hortonworks.faas.spark.predictor.inference_engine.analytic.common._
import com.hortonworks.faas.spark.predictor.inference_engine.configuration.ConfigurationOptionMap
import com.hortonworks.faas.spark.predictor.inference_engine.task.inference_engine_master
import com.hortonworks.faas.spark.predictor.schema_crawler.persistor.MetaDataPersistorOptions

/**
  * Created by njayakumar on 5/16/2018.
  */
class InferenceEngineOptions(val task:String,
                             val analytic_type: String,
                             val mdbenvironment: String,
                             val mdbservice: String)  {
  def this( t:String, cdfw:CommonDataFrameWriterOption , mdpo:MetaDataPersistorOptions) {
    this( t, mdpo.analytic_type, mdpo.mdbenvironment, mdpo.mdbservice)
  }

  def isValid(): Boolean = {
    true
  }
}

object InferenceEngineOptions {
  val TASK_KEY="task"

  def apply(args: Array[String]): InferenceEngineOptions = {
    apply(ConfigurationOptionMap(args))
  }

  def apply(options: ConfigurationOptionMap): InferenceEngineOptions = {

    val cdfw:CommonDataFrameWriterOption = CommonDataFrameWriterOption(options)
    val mdpo:MetaDataPersistorOptions = MetaDataPersistorOptions(options)

    val t:String = if(options.opts.contains(TASK_KEY) && options.opts(TASK_KEY).nonEmpty) options.opts(TASK_KEY)(0) else inference_engine_master.TASK

    new InferenceEngineOptions(t, cdfw, mdpo)
  }

  def printUsage(): Unit = {
    CommonDataFrameWriterOption.printUsage()

    println(s"${TASK_KEY} | Task to perform : { ${inference_engine_master.TASK}, ${inference_engine_master.TASK}, ${inference_engine_master.TASK} }; Default: ${inference_engine_master.TASK}")
  }
}
