package com.hortonworks.faas.spark.predictor.schema_crawler


import java.sql.Timestamp

import com.hortonworks.faas.spark.connector.hana.util.HanaDbConnectionInfo
import com.hortonworks.faas.spark.predictor.inference_engine.analytic.common.analytic.AdvancedAnalyticType
import com.hortonworks.faas.spark.predictor.schema_crawler.persistor.{MetaDataPersistor, MetaDataPersistorOptions}
import com.hortonworks.faas.spark.predictor.schema_crawler.task.schema_crawler_master
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
object SchemaCrawler extends ExecutionTiming with Logging
  with DfsUtils
  with SparkUtils {

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
    val opts: SchemaCrawlerOptions = SchemaCrawlerOptions(args)

    if (!opts.isValid()) {
      SchemaCrawlerOptions.printUsage()
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

    val sparkBuilder = createSparkBuilder(
      s"Schema Crawler - Dorsata",
      conf,
      args,
      -6)
    val spark = sparkBuilder.getOrCreate()

    val current_time: Timestamp = new Timestamp(DateTime.now().toDate.getTime)

    try {
      val output_ds = opts.task match {
        case schema_crawler_master.TASK => {
          val result_df = AdvancedAnalyticType.withNameWithDefault(opts.analytic_type) match {
            case AdvancedAnalyticType.HANA => {
              // step 1: get Hana meta data for the database object name
              val ds = time(s"run task for ${schema_crawler_master.TASK} and for the analytic type ${AdvancedAnalyticType.HANA.toString}",
                schema_crawler_master.getHanaMetaData(spark, dbName, opts.src_dbo_name, current_time))
              ds
            }
            case _ =>
              val d: RDD[Row] = spark.sparkContext.parallelize(Seq[Row](Row.fromSeq(Seq("Unknown advanced analytic type"))))
              spark.createDataFrame(d, StructType(StructField("ERROR", StringType, nullable = true) :: Nil))
          }
          result_df
        }
        case _ =>
          val d: RDD[Row] = spark.sparkContext.parallelize(Seq[Row](Row.fromSeq(Seq("Unknown task type"))))
          spark.createDataFrame(d, StructType(StructField("ERROR", StringType, nullable = true) :: Nil))

      }

      output_ds.printSchema

      // Step 2: Persist the metadata
      val mdpopts: MetaDataPersistorOptions = MetaDataPersistorOptions(args)
      MetaDataPersistor.persist(output_ds, spark, mdpopts)


    } finally {
      //make sure to call spark.stop so the history works
      time("Stopping Spark", {
        spark.stop()
      })
    }

    logDebug("""Done processing ...""")
  }
}