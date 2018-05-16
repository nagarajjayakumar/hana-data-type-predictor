package com.hortonworks.faas.spark.predictor.inference_engine

import java.sql.Timestamp

import com.hortonworks.faas.spark.predictor.inference_engine.task.inference_engine_master
import com.hortonworks.faas.spark.predictor.util._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime


/**
  * Inference Engine to infer the data type from HANA Samples.
  *
  * Multiple Sampling method
  *   1) equally weighted Sample
  *   2) population size for each group "on the fly":
  *   3) stratified reservoir sampling
  *   4) stratified constant proportion
  *
  */
object InferenceEngine extends ExecutionTiming with Logging
  with DfsUtils
  with SparkUtils {

  def main(args: Array[String]): Unit = {
    val opts: InferenceEngineOptions = InferenceEngineOptions(args)

    if (!opts.isValid()) {
      InferenceEngineOptions.printUsage()
      System.exit(1)
    }

    val sparkBuilder = createSparkBuilder(
      s"Inference Engine ",
      args,
      6)
    val spark = sparkBuilder.getOrCreate()

    val conf: SparkConf = new SparkConf().setAppName("RDFApp")
    val sc: SparkContext = new SparkContext(conf)
    val hiveContext: HiveContext = new HiveContext(sc)

    val current_time: Timestamp = new Timestamp(DateTime.now().toDate.getTime)

    try {
      val output_df = opts.task match {
        case inference_engine_master.TASK =>
          time(s"run task for ${inference_engine_master.TASK}",
            inference_engine_master.getData(spark, current_time))
      }


    } finally {
      //make sure to call spark.stop so the history works
      time("Stopping Spark", {
        spark.stop()
      })
    }

    logDebug("""Done processing ...""")
  }
}
