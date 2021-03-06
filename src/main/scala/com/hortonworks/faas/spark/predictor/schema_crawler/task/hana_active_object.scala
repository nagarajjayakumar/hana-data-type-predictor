package com.hortonworks.faas.spark.predictor.schema_crawler.task

import java.sql.Timestamp

import com.hortonworks.faas.spark.predictor.model.HanaActiveObject
import com.hortonworks.faas.spark.predictor.schema_crawler.SchemaCrawlerOptions
import com.hortonworks.faas.spark.predictor.xml.models.LogicalModelAttribute
import com.hortonworks.faas.spark.predictor.xml.parser.XmlParser
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.xml.XML

/**
  * Created by njayakumar on 5/16/2018.
  */
object hana_active_object {

  val TASK: String = "hana_active_object"

  val hana_active_object_query: String = "select * from \"_SYS_REPO\".\"ACTIVE_OBJECT\" where lower(object_suffix) in ('calculationview', 'attributeview', 'analyticview')  "


  def getData(spark: SparkSession,
              opts: SchemaCrawlerOptions,
              current_time: Timestamp): Dataset[HanaActiveObject] = {


    val dboname = opts.src_dbo_name
    import spark.implicits._
    val Array(package_id, object_name, _*) = dboname.split("/")
    val whereClause = "and package_id like '".concat(package_id).concat("%' and object_name like '").concat(object_name).concat("%'")
    val sql = hana_active_object_query.concat(whereClause)
    val df = spark
      .read
      .format("com.hortonworks.faas.spark.connector")
      .options(Map("query" -> (sql)))
      .load()

    df.as[HanaActiveObject]

  }

  def getHeadData(spark: SparkSession,
                  opts: SchemaCrawlerOptions,
                  current_time: Timestamp): HanaActiveObject = {
    val ds = getData(spark, opts, current_time)
    ds.head
  }


  def getHeadData(ds: Dataset[HanaActiveObject]) : HanaActiveObject= {
    ds.head
  }

  def parseMetaDataXml(xmlString: String): Array[LogicalModelAttribute] = {

    val xmlSource = XML.loadString(xmlString)
    val logicalModelAttribute = XmlParser.parse(xmlSource)(LogicalModelAttribute.xmlRead)
    logicalModelAttribute.toArray
  }
}
