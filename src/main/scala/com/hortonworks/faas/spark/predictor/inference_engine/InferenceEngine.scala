package com.hortonworks.faas.spark.predictor.inference_engine

import java.sql.Timestamp

import com.hortonworks.faas.spark.predictor.inference_engine.task.inference_engine_master
import com.hortonworks.faas.spark.predictor.util._
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime


/**
  * Acquires a SAS file from S3, uses readstat to convert it to CSV, and then loads it into a hive table in the specified schema. Temporary files are cleaned up
  * automatically.
  *
  *
  * Sample spark-submit call for this when running locally (using /tmp as temp):
  * spark-submit --master "local[*]" --packages org.apache.hadoop:hadoop-aws:2.7.3 --class ConvertSasToHive ./hive-spark_2.11-1.0.jar s3a://cafe-data-factory-sample-data /rwe_demo/m2_demo lb.sas7bdat matt_test lb /tmp "local[*]" "file://tmp"
  *
  * Sample spark-submit call for this on the HDP dev cluster (using /dev/shm as temp):
  * spark-submit --master "yarn" --class ConvertSasToHive ./hive-spark_2.11-1.0.jar s3a://cafe-data-factory-sample-data /rwe_demo/m2_demo lb.sas7bdat matt_test lb /dev/shm
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
    //val hiveContext: HiveContext = new HiveContext(sc)

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
