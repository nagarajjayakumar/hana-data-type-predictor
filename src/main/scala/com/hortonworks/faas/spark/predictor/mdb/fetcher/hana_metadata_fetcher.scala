package com.hortonworks.faas.spark.predictor.mdb.fetcher

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.orm.service.Connection
import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import com.hortonworks.faas.spark.predictor.util.Logging
import com.hortonworks.faas.spark.predictor.xml.models.LogicalModelAttribute
import com.hortonworks.faas.spark.predictor.xml.parser.XmlParser
import org.apache.spark.sql.SparkSession
import scalikejdbc.{DB, NamedDB}

import scala.xml.XML


class hana_metadata_fetcher(val spark: SparkSession,
                            val mdpopts: MetaDataBaseOptions,
                            val namespace: String,
                            val packge_id: String,
                            val dboname: String ) extends Logging with DBSettings with Connection {

  def isValid(): Boolean = {
    true
  }

  // Following env and db is for the metadata
  def env(): String = mdpopts.mdbenvironment

  // Following env and db is for the metadata
  def db(): DB = NamedDB(mdpopts.mdbservice).toDB()

  def fetchByName(): Unit = {
  }

  def parseMetaDataXml(xmlString: String): Array[LogicalModelAttribute] = {
    val xmlSource = XML.loadString(xmlString)
    val logicalModelAttribute = XmlParser.parse(xmlSource)(LogicalModelAttribute.xmlRead)
    logicalModelAttribute.toArray
  }

}

object hana_metadata_fetcher {

  def apply( spark: SparkSession,
             mdpopts: MetaDataBaseOptions,
             namespace: String,
             packge_id: String,
             dboname: String ): hana_metadata_fetcher = {
    new hana_metadata_fetcher( spark, mdpopts, namespace, packge_id,dboname)
  }

}
