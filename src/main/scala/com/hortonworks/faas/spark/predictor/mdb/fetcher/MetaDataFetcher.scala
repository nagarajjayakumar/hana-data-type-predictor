package com.hortonworks.faas.spark.predictor.mdb.fetcher

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.{SourceDbActiveObject, SourceDbActiveObjectDetail}
import com.hortonworks.faas.spark.predictor.orm.service.Connection
import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.SparkSession
import scalikejdbc.{DB, NamedDB}


class MetaDataFetcher(val spark: SparkSession,
                      val mdpopts: MetaDataBaseOptions) extends Logging with DBSettings with Connection {

  def isValid(): Boolean = {
    true
  }

  // Following env and db is for the metadata
  def env(): String = mdpopts.mdbenvironment

  // Following env and db is for the metadata
  def db(): DB = NamedDB(mdpopts.mdbservice).toDB()

  def fetch(): SourceDbActiveObject = {

    SourceDbActiveObject.findAllBy("db_object_name like")

  }


}


}

object MetaDataFetcher {

  def fetchActiveObject(spark: SparkSession, mdpo: MetaDataBaseOptions): Unit = {

  }
}
