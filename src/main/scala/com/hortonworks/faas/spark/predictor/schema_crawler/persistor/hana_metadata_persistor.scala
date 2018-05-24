package com.hortonworks.faas.spark.predictor.schema_crawler.persistor

import com.hortonworks.faas.spark.predictor.orm.service.Connection
import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import com.hortonworks.faas.spark.predictor.schema_crawler.model.HanaActiveObject
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import scalikejdbc.{DB, NamedDB}

class hana_metadata_persistor(val ds: Dataset[HanaActiveObject],
                              val spark: SparkSession,
                              val environment: String,
                              val dbService: String) extends Logging with DBSettings with Connection {

  def isValid(): Boolean = {
    true
  }

  def env(): String = environment

  def db(): DB = NamedDB(dbService).toDB()

  def persist(): Unit = {
    val hao: HanaActiveObject = ds.head
  }

}

object hana_metadata_persistor {

  def apply(ds: Dataset[HanaActiveObject], spark: SparkSession, environment: String, dbService: String): hana_metadata_persistor = {
    new hana_metadata_persistor(ds, spark, environment, dbService)
  }


}
