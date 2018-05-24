package com.hortonworks.faas.spark.predictor.schema_crawler.persistor

import java.util.Calendar

import com.hortonworks.faas.spark.predictor.orm.service.Connection
import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import com.hortonworks.faas.spark.predictor.schema_crawler.model.{HanaActiveObject, SourceDbActiveObject, SourceDbActiveObjectDetail}
import com.hortonworks.faas.spark.predictor.util.{CommonData, Logging}
import com.hortonworks.faas.spark.predictor.xml.models.LogicalModelAttribute
import com.hortonworks.faas.spark.predictor.xml.parser.XmlParser
import org.apache.spark.sql.{Dataset, SparkSession}
import scalikejdbc.{DB, NamedDB}

import scala.xml.XML

class hana_metadata_persistor(val ds: Dataset[HanaActiveObject],
                              val spark: SparkSession,
                              val environment: String,
                              val dbService: String) extends Logging with DBSettings with Connection {

  def isValid(): Boolean = {
    true
  }
  // Following env and db is for the metadata
  def env(): String = environment
  // Following env and db is for the metadata
  def db(): DB = NamedDB(dbService).toDB()

  def persist(): Unit = {
    val hao: HanaActiveObject = ds.head

    val haoid = SourceDbActiveObject.createWithAttributes('namespace -> hao.PACKAGE_ID,
      'dbObjectName -> hao.OBJECT_NAME,
      'dbObjectType -> hao.OBJECT_SUFFIX,
      'dbObjectTypeSuffix -> hao.OBJECT_SUFFIX,
      'version -> 1,
      'activatedAt -> Calendar.getInstance.getTime,
      'activatedBy -> System.getProperty("user.name"),
      'isActive -> true)

    val haodetails = parseMetaDataXml(hao.CDATA.get)

    for (haodetail: LogicalModelAttribute <- haodetails){
      
      SourceDbActiveObjectDetail.createWithAttributes(
        'columnName -> haodetail.id,
        'haoid -> haoid,
        'isKey -> haodetail.key,
        'col_order -> haodetail.order,
        'attributeHierarchyActive -> haodetail.attributeHierarchyActive,
        'displayAttribute -> haodetail.displayAttribute,
        'defaultDescription -> haodetail.logicalModelAttributesAttribDesc.head.defaultDescription,
        'sourceObjectName -> haodetail.logicalModelAttributesAttribKeyMapping.head.columnObjectName,
        'sourceColumnName ->  haodetail.logicalModelAttributesAttribKeyMapping.head.columnName,
        'isRequiredForFlow -> true)

    }


  }

  def parseMetaDataXml(xmlString: String): Array[LogicalModelAttribute] = {
    val xmlSource = XML.loadString(xmlString)
    val logicalModelAttribute = XmlParser.parse(xmlSource)(LogicalModelAttribute.xmlRead)
    logicalModelAttribute.toArray
  }

}

object hana_metadata_persistor {

  def apply(ds: Dataset[HanaActiveObject], spark: SparkSession, environment: String, dbService: String): hana_metadata_persistor = {
    new hana_metadata_persistor(ds, spark, environment, dbService)
  }


}
