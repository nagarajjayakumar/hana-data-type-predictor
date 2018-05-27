package com.hortonworks.faas.spark.predictor.orm.service

import java.util.Calendar

import com.hortonworks.faas.spark.predictor.mdb.model.{SourceDbActiveObject, SourceDbActiveObjectDetail}
import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import org.scalatest.Matchers
import scalikejdbc.{DB, NamedDB}

object HanadbSpec extends DBSettings with Matchers with Connection with CreateTables {

  def env(): String = "development_mysql"

  def db(): DB = NamedDB('service).toDB()


  def main(args: Array[String]): Unit = {

    val haoid = SourceDbActiveObject.createWithAttributes('namespace -> "testService",
      'dbObjectName -> "testObjectName",
      'dbObjectType -> "testObjectType",
      'dbObjectTypeSuffix -> "testObjectTypeSuffix",
      'version -> 1,
      'activatedAt -> Calendar.getInstance.getTime,
      'activatedBy -> "testUser",
      'isActive -> true)


    SourceDbActiveObjectDetail.createWithAttributes('columnName -> "testColumnName1", 'haoid -> haoid,
      'isKey -> true, 'col_order -> 1, 'attributeHierarchyActive -> true, 'displayAttribute -> false,
      'defaultDescription -> "testDescription1", 'sourceObjectName -> "testSrcObjectName",
      'sourceColumnName -> "testSrcColumnName", 'isRequiredForFlow -> true)

    SourceDbActiveObjectDetail.createWithAttributes('columnName -> "testColumnName2", 'haoid -> haoid,
      'isKey -> true, 'col_order -> 1, 'attributeHierarchyActive -> true, 'displayAttribute -> false,
      'defaultDescription -> "testDescription2", 'sourceObjectName -> "testSrcObjectName",
      'sourceColumnName -> "testSrcColumnName", 'isRequiredForFlow -> true)

    SourceDbActiveObjectDetail.createWithAttributes('columnName -> "testColumnName3", 'haoid -> haoid,
      'isKey -> false, 'col_order -> 1, 'attributeHierarchyActive -> true, 'displayAttribute -> false,
      'defaultDescription -> "testDescription3", 'sourceObjectName -> "testSrcObjectName",
      'sourceColumnName -> "testSrcColumnName", 'isRequiredForFlow -> true)

    val hao = SourceDbActiveObject.joins(SourceDbActiveObject.hanaDbActiveObjectDetails).findAll().head
    println(hao.hanaDbActiveObjectDetails)

    val haod = SourceDbActiveObjectDetail.deleteById(hao.hanaDbActiveObjectDetails.head.id)
    println(haod)
    val hao1 = SourceDbActiveObject.joins(SourceDbActiveObject.hanaDbActiveObjectDetails).findAll().head

    println(hao1.hanaDbActiveObjectDetails)
  }


}

