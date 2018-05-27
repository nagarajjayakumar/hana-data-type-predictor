package com.hortonworks.faas.spark.predictor.mdb.persistor

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.orm.service.Connection
import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import scalikejdbc.{DB, NamedDB}

class hana_metadata_details_persistor(val spark: SparkSession,
                                      val mdpopts: MetaDataBaseOptions,
                                      saod: List[SourceDbActiveObjectDetail],
                                      schemaMap: Map[String, StructType]) extends Logging with DBSettings with Connection {

  def isValid(): Boolean = {
    true
  }

  // Following env and db is for the metadata
  def env(): String = mdpopts.mdbenvironment

  // Following env and db is for the metadata
  def db(): DB = NamedDB(mdpopts.mdbservice).toDB()

  def updateDbActiveObjectDetails(): Unit = {

  }

}

object hana_metadata_details_persistor {

  def apply( spark: SparkSession,
             mdpopts: MetaDataBaseOptions,
             saod: List[SourceDbActiveObjectDetail],
             schemaMap: Map[String, StructType]): hana_metadata_details_persistor = {
    new hana_metadata_details_persistor( spark, mdpopts, saod,schemaMap)
  }


}
