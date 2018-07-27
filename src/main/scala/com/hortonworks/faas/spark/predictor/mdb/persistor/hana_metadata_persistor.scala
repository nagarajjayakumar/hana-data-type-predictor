package com.hortonworks.faas.spark.predictor.mdb.persistor

import java.util.Calendar

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.{SourceDbActiveObject, SourceDbActiveObjectDetail}
import com.hortonworks.faas.spark.predictor.model.HanaActiveObject
import com.hortonworks.faas.spark.predictor.orm.service.Connection
import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import com.hortonworks.faas.spark.predictor.util.{CommonData, Logging}
import com.hortonworks.faas.spark.predictor.xml.models.LogicalModelAttribute
import com.hortonworks.faas.spark.predictor.xml.parser.XmlParser
import org.apache.spark.sql.{Dataset, SparkSession}
import scalikejdbc.{DB, NamedDB}

import scala.xml.XML

class hana_metadata_persistor(val spark: SparkSession,
                              val mdpopts: MetaDataBaseOptions,
                              val ds: Dataset[HanaActiveObject]) extends Logging with DBSettings with Connection {


  // initialize logger
  log

  def isValid(): Boolean = {
    true
  }

  // Following env and db is for the metadata
  def env(): String = mdpopts.mdbenvironment

  // Following env and db is for the metadata
  def db(): DB = NamedDB(mdpopts.mdbservice).toDB()

  def persist(): Unit = {
    val hao: HanaActiveObject = ds.head

    val haoid = SourceDbActiveObject.createWithAttributes('namespace -> mdpopts.src_namespace,
      'packageId -> hao.PACKAGE_ID,
      'dbObjectName -> hao.OBJECT_NAME,
      'dbObjectType -> hao.OBJECT_SUFFIX,
      'dbObjectTypeSuffix -> hao.OBJECT_SUFFIX,
      'version -> 1,
      'activatedAt -> Calendar.getInstance.getTime,
      'activatedBy -> System.getProperty("user.name"),
      'isActive -> true)

    val haodetails = parseMetaDataXml(hao.CDATA.get)

    for (haodetail: LogicalModelAttribute <- haodetails) {

      if (! CommonData.getBooleanFromStringForDBKeys(haodetail.hidden))
      {
        SourceDbActiveObjectDetail.createWithAttributes(
          'columnName -> haodetail.id,
          'haoid -> haoid,
          'isKey -> CommonData.getBooleanFromStringForDBKeys(haodetail.key),
          'col_order -> haodetail.order,
          'attributeHierarchyActive -> CommonData.getBooleanFromStringForDBKeys(haodetail.attributeHierarchyActive),
          'displayAttribute -> CommonData.getBooleanFromStringForDBKeys(haodetail.displayAttribute),
          'defaultDescription -> haodetail.logicalModelAttributesAttribDesc.head.defaultDescription,
          'sourceObjectName -> haodetail.logicalModelAttributesAttribKeyMapping.head.columnObjectName,
          'sourceColumnName -> haodetail.logicalModelAttributesAttribKeyMapping.head.columnName,
          'isRequiredForFlow -> true)
      }

    }


  }

  def parseMetaDataXml(xmlString: String): Array[LogicalModelAttribute] = {
    logDebug(s"CDATA xmlString ${xmlString}")
    val xmlSource = XML.loadString(xmlString)
    val logicalModelAttribute = XmlParser.parse(xmlSource)(LogicalModelAttribute.xmlRead)
    logicalModelAttribute.toArray
  }

}

object hana_metadata_persistor {

  def apply( spark: SparkSession,
             mdpopts: MetaDataBaseOptions,
             ds: Dataset[HanaActiveObject]): hana_metadata_persistor = {
    new hana_metadata_persistor( spark, mdpopts, ds)
  }


}
