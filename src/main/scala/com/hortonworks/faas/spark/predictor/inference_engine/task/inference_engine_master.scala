package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp

import com.hortonworks.faas.spark.predictor.inference_engine.InferenceEngineOptions
import com.hortonworks.faas.spark.predictor.inference_engine.analytic.common.analytic.{AdvancedAnalyticType, SamplingTechniqType}
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * Created by njayakumar on 5/26/2018.
  */
object inference_engine_master extends Logging {

  // initialize log file.
  log

  val TASK: String = "inference_engine_master"

  val ANALYTIC_TYPE = AdvancedAnalyticType.HANA

  val SRCDBONAME = "default/default"

  val SAMPLING_TECHNIQ = "full"

  val SAMPLING_PERCENTAGE = "1"

  val RSVR_SAMPLE_SIZE = "1"

  val RUNTIME_ENV = "local"

  val SRCNAMESPACE = "_SYS_BIC"


  def inferSchema(spark: SparkSession, opts: InferenceEngineOptions,
                  dbaoDetails: List[SourceDbActiveObjectDetail], keys: List[SourceDbActiveObjectDetail],
                  current_time: Timestamp): Map[String, StructType] = {
    val output_schema = SamplingTechniqType.withNameWithDefault(opts.sampling_techniq) match {
      case SamplingTechniqType.STRT_RSVR_SMPL => {

        val sampleData: DataFrame = hana_stratified_reservoir_sampler.getData(spark, opts, dbaoDetails, keys, current_time)
        logDebug(s"Data schema after applying ${SamplingTechniqType.STRT_RSVR_SMPL.toString} ${sampleData.printSchema}")

        val inferData: DataFrame = hana_stratified_reservoir_sampler.inferSchema(spark, sampleData, current_time)
        logDebug(s"Data schema after Inferring data type ${SamplingTechniqType.STRT_RSVR_SMPL.toString} ${inferData.printSchema}")

        var bothSrcAndInferSchema = scala.collection.mutable.Map[String, StructType]()
        bothSrcAndInferSchema += ("originalSchema" -> sampleData.schema)
        bothSrcAndInferSchema += ("inferSchema" -> inferData.schema)
        bothSrcAndInferSchema.toMap
      }
      case SamplingTechniqType.STRT_CONST_PROP => {

        val sampleData: DataFrame = hana_stratified_constant_proportion.getData(spark, opts, dbaoDetails, keys, current_time)
        logDebug(s"Data schema after applying ${SamplingTechniqType.STRT_CONST_PROP.toString} ${sampleData.printSchema}")

        val inferData: DataFrame = hana_stratified_constant_proportion.inferSchema(spark, sampleData, current_time)
        logDebug(s"Data schema after Inferring data type ${SamplingTechniqType.STRT_CONST_PROP.toString} ${inferData.printSchema}")

        var bothSrcAndInferSchema = scala.collection.mutable.Map[String, StructType]()
        bothSrcAndInferSchema += ("originalSchema" -> sampleData.schema)
        bothSrcAndInferSchema += ("inferSchema" -> inferData.schema)
        bothSrcAndInferSchema.toMap
      }
      case SamplingTechniqType.STRT_DYNMC_POPL => {

        val sampleData: DataFrame = hana_stratified_dynamic_population.getData(spark, opts, dbaoDetails, keys, current_time)
        logDebug(s"Data schema after applying ${SamplingTechniqType.STRT_DYNMC_POPL.toString} ${sampleData.printSchema}")

        val inferData: DataFrame = hana_stratified_dynamic_population.inferSchema(spark, sampleData, current_time)
        logDebug(s"Data schema after Inferring data type ${SamplingTechniqType.STRT_DYNMC_POPL.toString} ${inferData.printSchema}")

        var bothSrcAndInferSchema = scala.collection.mutable.Map[String, StructType]()
        bothSrcAndInferSchema += ("originalSchema" -> sampleData.schema)
        bothSrcAndInferSchema += ("inferSchema" -> inferData.schema)
        bothSrcAndInferSchema.toMap
      }
      case SamplingTechniqType.RNDM_SMPL_SYS => {

        val sampleData: DataFrame = hana_random_uniform_system.getData(spark, opts, dbaoDetails, keys, current_time)
        logDebug(s"Data schema after applying ${SamplingTechniqType.RNDM_SMPL_SYS.toString} ${sampleData.printSchema}")

        val inferData: DataFrame = hana_random_uniform_system.inferSchema(spark, sampleData, current_time)
        logDebug(s"Data schema after Inferring data type ${SamplingTechniqType.RNDM_SMPL_SYS.toString} ${inferData.printSchema}")

        var bothSrcAndInferSchema = scala.collection.mutable.Map[String, StructType]()
        bothSrcAndInferSchema += ("originalSchema" -> sampleData.schema)
        bothSrcAndInferSchema += ("inferSchema" -> inferData.schema)
        bothSrcAndInferSchema.toMap
      }
      case SamplingTechniqType.RNDM_SMPL_BER => {

        val sampleData: DataFrame = hana_random_uniform_bernoulli.getData(spark, opts, dbaoDetails, keys, current_time)
        logDebug(s"Data schema after applying ${SamplingTechniqType.RNDM_SMPL_BER.toString} ${sampleData.printSchema}")

        val inferData: DataFrame = hana_random_uniform_bernoulli.inferSchema(spark, sampleData, current_time)
        logDebug(s"Data schema after Inferring data type ${SamplingTechniqType.RNDM_SMPL_BER.toString} ${inferData.printSchema}")

        var bothSrcAndInferSchema = scala.collection.mutable.Map[String, StructType]()
        bothSrcAndInferSchema += ("originalSchema" -> sampleData.schema)
        bothSrcAndInferSchema += ("inferSchema" -> inferData.schema)
        bothSrcAndInferSchema.toMap
      }
      case SamplingTechniqType.NO_SMPL => {

        val sampleData: DataFrame = hana_no_sampling_techniq.getData(spark, opts, dbaoDetails, keys, current_time)
        logDebug(s"Data schema after applying ${SamplingTechniqType.NO_SMPL.toString} ${sampleData.printSchema}")

        val inferData: DataFrame = hana_no_sampling_techniq.inferSchema(spark, sampleData, current_time)
        logDebug(s"Data schema after Inferring data type ${SamplingTechniqType.NO_SMPL.toString} ${inferData.printSchema}")

        var bothSrcAndInferSchema = scala.collection.mutable.Map[String, StructType]()
        bothSrcAndInferSchema += ("originalSchema" -> sampleData.schema)
        bothSrcAndInferSchema += ("inferSchema" -> inferData.schema)
        bothSrcAndInferSchema.toMap
      }
      case SamplingTechniqType.RNDM_SMPL_CUSTOM => {

        val sampleData: DataFrame = hana_random_uniform_custom.getData(spark, opts, dbaoDetails, keys, current_time)
        logDebug(s"Data schema after applying ${SamplingTechniqType.RNDM_SMPL_CUSTOM.toString} ${sampleData.printSchema}")

        val inferData: DataFrame = hana_random_uniform_custom.inferSchema(spark, sampleData, current_time)
        logDebug(s"Data schema after Inferring data type ${SamplingTechniqType.RNDM_SMPL_CUSTOM.toString} ${inferData.printSchema}")

        var bothSrcAndInferSchema = scala.collection.mutable.Map[String, StructType]()
        bothSrcAndInferSchema += ("originalSchema" -> sampleData.schema)
        bothSrcAndInferSchema += ("inferSchema" -> inferData.schema)
        bothSrcAndInferSchema.toMap
      }
      case _ =>
        val d: RDD[Row] = spark.sparkContext.parallelize(Seq[Row](Row.fromSeq(Seq("Unknown Sampling techniq "))))
        spark.createDataFrame(d, StructType(StructField("ERROR", StringType, nullable = true) :: Nil))
        scala.collection.mutable.Map[String, StructType]().toMap
    }

    output_schema
  }

  def updateDbActiveObjectDetailsWithSourceDataType(dbaoDetails: List[SourceDbActiveObjectDetail],
                                                    schema: StructType): List[SourceDbActiveObjectDetail] = {

    var finalDboaDetails = new ListBuffer[SourceDbActiveObjectDetail]()

    for ((dboaDetail: SourceDbActiveObjectDetail, index) <- dbaoDetails.zipWithIndex) {
      val fieldDataType = schema(dboaDetail.sourceColumnName)
      finalDboaDetails += dboaDetail.copy(sourceDataType = fieldDataType.dataType.simpleString)
    }

    dbaoDetails

  }

  def updateDbActiveObjectDetailsWithInferDataType(dbaoDetails: List[SourceDbActiveObjectDetail],
                                                   schema: StructType): List[SourceDbActiveObjectDetail] = {

    var finalDboaDetails = new ListBuffer[SourceDbActiveObjectDetail]()

    for ((dboaDetail: SourceDbActiveObjectDetail, index) <- dbaoDetails.zipWithIndex) {
      val fieldDataType = schema(dboaDetail.sourceColumnName)
      finalDboaDetails += dboaDetail.copy(inferDataType = fieldDataType.dataType.simpleString)
    }

    dbaoDetails

  }


}
