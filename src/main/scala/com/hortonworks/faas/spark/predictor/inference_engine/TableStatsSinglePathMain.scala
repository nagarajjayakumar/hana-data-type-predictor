package com.hortonworks.faas.spark.predictor.inference_engine

import com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngine.{createSparkBuilder, local, masterConnectionInfo}
import com.hortonworks.faas.spark.predictor.model.FirstPassStatsModel
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
/**
  * Created by ted.malaska on 6/27/15.
  */
object TableStatsSinglePathMain {
  def main(args: Array[String]): Unit = {

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

    val sparkBuilder = createSparkBuilder(
      s"Inference Engine - Naarai",
      conf,
      args,
      -6)
    val spark = sparkBuilder.getOrCreate()

    val sparkSession = SparkSession.builder.getOrCreate()

    val sc = sparkSession.sparkContext

    try {
      val sqlContext = sparkSession.sqlContext


      val schema =
        StructType(
          Array(
            StructField("id", LongType, true),
            StructField("name", StringType, true),
            StructField("age", LongType, true),
            StructField("gender", StringType, true),
            StructField("height", LongType, true),
            StructField("job_title", StringType, true)
          )
        )

      val rowRDD = sc.parallelize(Array(
        Row(1l, "Name.1", 20l, "M", 6l, "dad"),
        Row(2l, "Name.2", 20l, "F", 5l, "mom"),
        Row(3l, "Name.3", 20l, "F", 5l, "mom"),
        Row(4l, "Name.4", 20l, "M", 5l, "mom"),
        Row(5l, "Name.5", 10l, "M", 4l, "kid"),
        Row(6l, "Name.6", 8l, "M", 3l, "kid")))

      val df = sqlContext.createDataFrame(rowRDD, schema)

      val firstPassStats = TableStatsSinglePathMain.getFirstPassStat(sparkSession, rowRDD, df)

      print(firstPassStats)

    } finally {
      sc.stop()
    }
  }

  def getFirstPassStat(sparkSession: SparkSession, rdd: RDD[Row], df: DataFrame): FirstPassStatsModel = {
    val schema = df.schema

    import sparkSession.implicits._

    //Part B.1
    val columnValueCounts = rdd.flatMap(r =>
      (0 until schema.length).map { idx =>
        //((columnIdx, cellValue), count)
        ((idx, r.get(idx)), 1l)
      }
    ).reduceByKey(_ + _) //This is like word count


    //Part C
    val firstPassStats = columnValueCounts.mapPartitions[FirstPassStatsModel] { it =>
      val firstPassStatsModel = new FirstPassStatsModel()
      it.foreach { case ((columnIdx, columnVal), count) =>
        firstPassStatsModel += (columnIdx, columnVal, count)
      }
      Iterator(firstPassStatsModel)
    }.reduce { (a, b) => //Part D
      a += (b)
      a
    }

    firstPassStats
  }
}