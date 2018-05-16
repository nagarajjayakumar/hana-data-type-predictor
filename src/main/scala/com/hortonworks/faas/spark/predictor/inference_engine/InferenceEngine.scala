package com.hortonworks.faas.spark.predictor.inference_engine

import java.sql.Timestamp

import com.hortonworks.faas.spark.predictor.inference_engine.task.inference_engine_master
import com.hortonworks.faas.spark.predictor.util.{ExecutionTiming, Logging}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

/**
  * Created by njayakumar on 5/16/2018.
  */
object InferenceEngine extends ExecutionTiming with Logging {
  def main(args: Array[String]): Unit = {
    val opts: InferenceEngineOptions = InferenceEngineOptions(args)

    if (!opts.isValid()) {
      InferenceEngineOptions.printUsage()
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("Inference Engine")
      .enableHiveSupport()
      .getOrCreate()

    val conf: SparkConf = new SparkConf().setAppName("RDFApp")
    val sc: SparkContext = new SparkContext(conf)
    //val hiveContext: HiveContext = new HiveContext(sc)

    val current_time: Timestamp = new Timestamp(DateTime.now().toDate.getTime)

    try {
      val output_df = opts.task match {
        case inference_engine_master.TASK =>
          time(s"run task for ${inference_engine_master.TASK}",
            inference_engine_master.getData(spark))
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
