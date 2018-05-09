package com.hortonworks.faas.spark.predictor.orm.service

import java.sql.Date
import java.util.Calendar

import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import org.scalatest.{Matchers, fixture}
import scalikejdbc.{AutoSession, DB, DBSession, NamedDB}
import scalikejdbc.scalatest.AutoRollback

object HanadbSpec extends    DBSettings with Matchers with Connection with CreateTables  {

   def env(): String = "development_mysql"
   def db(): DB = NamedDB('service).toDB()


  def main(args: Array[String]): Unit = {

    val haoid = HanaDbActiveObject.createWithAttributes('namespace -> "testService",
      'dbObjectName -> "testObjectName",
      'dbObjectType -> "testObjectType",
      'dbObjectTypeSuffix -> "testObjectTypeSuffix",
      'version -> 1,
      'activatedAt -> Calendar.getInstance.getTime,
      'activatedBy -> "testUser",
      'isActive -> true )



    HanaDbActiveObjectDetail.createWithAttributes('columnName -> "testColumnName1", 'haoid   -> haoid,
      'isKey -> true, 'col_order -> 1 , 'attributeHierarchyActive -> true, 'displayAttribute -> false,
      'defaultDescription -> "testDescription1", 'sourceObjectName -> "testSrcObjectName",
      'sourceColumnName -> "testSrcColumnName", 'isRequiredForFlow -> true)

    HanaDbActiveObjectDetail.createWithAttributes('columnName -> "testColumnName2", 'haoid   -> haoid,
      'isKey -> true, 'col_order -> 1 , 'attributeHierarchyActive -> true, 'displayAttribute -> false,
      'defaultDescription -> "testDescription2", 'sourceObjectName -> "testSrcObjectName",
      'sourceColumnName -> "testSrcColumnName", 'isRequiredForFlow -> true)

    HanaDbActiveObjectDetail.createWithAttributes('columnName -> "testColumnName3", 'haoid   -> haoid,
      'isKey -> false, 'col_order -> 1 , 'attributeHierarchyActive -> true, 'displayAttribute -> false,
      'defaultDescription -> "testDescription3", 'sourceObjectName -> "testSrcObjectName",
      'sourceColumnName -> "testSrcColumnName", 'isRequiredForFlow -> true)

    val hao = HanaDbActiveObject.joins(HanaDbActiveObject.hanaDbActiveObjectDetails).findAll().head
    println(hao.hanaDbActiveObjectDetails)

    val haod = HanaDbActiveObjectDetail.deleteById(hao.hanaDbActiveObjectDetails.head.id)
    println(haod)
    val hao1 = HanaDbActiveObject.joins(HanaDbActiveObject.hanaDbActiveObjectDetails).findAll().head

    println(hao1.hanaDbActiveObjectDetails)
  }



}

