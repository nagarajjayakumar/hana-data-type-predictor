package com.hortonworks.faas.spark.predictor.mdb.fetcher

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.{SourceDbActiveObject, SourceDbActiveObjectDetail}
import com.hortonworks.faas.spark.predictor.orm.service.Connection
import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.SparkSession
import scalikejdbc.{DB, NamedDB, _}


class hana_metadata_fetcher(val spark: SparkSession,
                            val mdopts: MetaDataBaseOptions,
                            val namespace: String,
                            val packge_id: String,
                            val dboname: String) extends Logging with DBSettings with Connection {

  def isValid(): Boolean = {
    true
  }

  // Following env and db is for the metadata
  def env(): String = mdopts.mdbenvironment

  // Following env and db is for the metadata
  def db(): DB = NamedDB(mdopts.mdbservice).toDB()

  def fetchDbActiveObjectByName = {
    val a = SourceDbActiveObject.defaultAlias
    val sao: SourceDbActiveObject = SourceDbActiveObject.where(sqls.eq(sqls.lower(a.packageId), packge_id.toLowerCase)
      .and.eq(sqls.lower(a.dbObjectName), dboname.toLowerCase)).apply().head
    sao
  }

  def fetchDbActiveObjectDetailByName(): List[SourceDbActiveObjectDetail] = {

    val sao: SourceDbActiveObject = fetchDbActiveObjectByName

    val a1 = SourceDbActiveObjectDetail.defaultAlias
    val saod: List[SourceDbActiveObjectDetail] = SourceDbActiveObjectDetail.where(sqls.eq(a1.haoid, sao.id)).apply()

    saod

  }

  def fetchDbActiveObjectKeysOnly(): List[SourceDbActiveObjectDetail] = {
    val sao: SourceDbActiveObject = fetchDbActiveObjectByName

    val a1 = SourceDbActiveObjectDetail.defaultAlias
    val saod: List[SourceDbActiveObjectDetail] = SourceDbActiveObjectDetail.where(sqls.eq(a1.haoid, sao.id).and.eq(a1.isKey, true)).apply

    saod
  }

}

object hana_metadata_fetcher {

  def apply(spark: SparkSession,
            mdopts: MetaDataBaseOptions) : hana_metadata_fetcher = {

    val namespace = mdopts.src_namespace
    val Array(package_id, db_object_name, _*) = mdopts.src_dbo_name.split("/")
    new hana_metadata_fetcher(spark, mdopts, namespace, package_id, db_object_name)
  }

}
