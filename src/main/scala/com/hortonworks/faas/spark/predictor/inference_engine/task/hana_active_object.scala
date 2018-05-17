package com.hortonworks.faas.spark.predictor.inference_engine.task

import java.sql.Timestamp

import com.hortonworks.faas.spark.predictor.xml.models.{Calculation, LogicalModelAttribute}
import com.hortonworks.faas.spark.predictor.xml.parser.XmlParser
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.xml.XML

/**
  * Created by njayakumar on 5/16/2018.
  */
object hana_active_object {

  val TASK: String = "hana_active_object"

  val hana_active_object_query: String = "SELECT * FROM s3_data.phm_serialized_engine"


  def getData(spark: SparkSession,  current_time: Timestamp): DataFrame = {
     spark
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map("query" -> (hana_active_object_query)))
      .load()
  }

  def parseXml(xmlString: String): LogicalModelAttribute = {

    val xmlSource = XML.loadString(xmlString)
    val logicalModelAttribute = XmlParser.parse(xmlSource)(LogicalModelAttribute.xmlRead)
    logicalModelAttribute.head
  }
}
