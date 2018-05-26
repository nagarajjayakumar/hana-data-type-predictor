package com.hortonworks.faas.spark.predictor.inference_engine

import java.sql.Timestamp

import com.hortonworks.faas.spark.connector.hana.util.HanaDbConnectionInfo
import com.hortonworks.faas.spark.predictor.inference_engine.task.inference_engine_master
import com.hortonworks.faas.spark.predictor.util._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.joda.time.DateTime


/**
  * Inference Engine to infer the data type from HANA Samples.
  *
  * Multiple Sampling method
  * 1) equally weighted Sample
  * 2) population size for each group "on the fly":
  * 3) stratified reservoir sampling
  * 4) stratified constant proportion
  *
  */
object InferenceEngine extends ExecutionTiming with Logging
  with DfsUtils
  with SparkUtils {

  //initialize logger
  log

  val dbName: String = {
    "_SYS_BIC"
  }

  val masterHost = sys.env.get("HANADB_HOST_TEST").getOrElse("127.0.0.1")
  //val masterHost = sys.env.get("MYSQLDB_HOST_TEST").getOrElse("127.0.0.1")
  val masterConnectionInfo: HanaDbConnectionInfo =
    HanaDbConnectionInfo(masterHost, 30015, "SYS_VDM", "Cnct2VDM4", dbName) // scalastyle:ignore
  val leafConnectionInfo: HanaDbConnectionInfo =
    HanaDbConnectionInfo(masterHost, 30015, "SYS_VDM", "Cnct2VDM4", dbName) // scalastyle:ignore

  val local: Boolean = true

  def main(args: Array[String]): Unit = {
    val opts: InferenceEngineOptions = InferenceEngineOptions(args)

    if (!opts.isValid()) {
      InferenceEngineOptions.printUsage()
      System.exit(1)
    }

    var conf = new SparkConf()
      .setAppName("HanaDb Connector Test")
      .set("spark.hanadb.host", masterConnectionInfo.dbHost)
      .set("spark.hanadb.port", masterConnectionInfo.dbPort.toString)
      .set("spark.hanadb.user", masterConnectionInfo.user)
      .set("spark.hanadb.password", masterConnectionInfo.password)
      .set("spark.hanadb.defaultDatabase", masterConnectionInfo.dbName)
      .set("spark.driver.host", "localhost")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.set("spark.ui.port", (4040 + scala.util.Random.nextInt(1000)).toString)

    if (local) {
      conf = conf.setMaster("local")
    }

    // set some negative value for the local run purpose
    // remove it @
    val sparkBuilder = createSparkBuilder(
      s"Inference Engine - Naarai",
      conf,
      args,
      -6)
    val spark = sparkBuilder.getOrCreate()

    val current_time: Timestamp = new Timestamp(DateTime.now().toDate.getTime)

    try {
      val output_df = opts.task match {
        case inference_engine_master.TASK =>
          time(s"run task for ${inference_engine_master.TASK}",
            inference_engine_master.getData(spark, current_time))
        case _ =>
          val d: RDD[Row] = spark.sparkContext.parallelize(Seq[Row](Row.fromSeq(Seq("Unknown task type"))))
          spark.createDataFrame(d, StructType(StructField("ERROR", StringType, nullable = true) :: Nil))

      }

      output_df.printSchema()

    } finally {
      //make sure to call spark.stop so the history works
      time("Stopping Spark", {
        spark.stop()
      })
    }

    logDebug("""Done processing ...""")
  }
}
