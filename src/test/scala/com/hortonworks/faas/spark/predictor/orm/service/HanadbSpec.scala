package com.hortonworks.faas.spark.predictor.orm.service

import java.sql.Date
import java.util.Calendar

import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import org.scalatest.{Matchers, fixture}
import scalikejdbc.{DB, DBSession, NamedDB}
import scalikejdbc.scalatest.AutoRollback

class HanadbSpec extends fixture.FunSpec with   DBSettings with Matchers with Connection with CreateTables with AutoRollback {

  override def env(): String = "development"
  override def db(): DB = NamedDB('service).toDB()

  override def fixture(implicit session: DBSession): Unit = {


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
      'isKey -> true, 'col_order -> 1 , 'attributeHierarchyActive -> true, 'displayAttribute -> false,
      'defaultDescription -> "testDescription3", 'sourceObjectName -> "testSrcObjectName",
      'sourceColumnName -> "testSrcColumnName", 'isRequiredForFlow -> true)

  }

  describe("hasMany without byDefault") {
    it("should work as expected") { implicit session =>
      val hao = HanaDbActiveObject.joins(HanaDbActiveObject.hanaDbActiveObjectDetails).findAll().head
      print(hao.hanaDbActiveObjectDetails)
      hao.hanaDbActiveObjectDetails.size should equal(3)
    }
  }

//  describe("belongsTo without byDefault") {
//    it("should work as expected") { implicit session =>
//      val app = Application.joins(Application.service).findAll().head
//      app.service.isDefined should equal(true)
//
//      val beforeService = Service.joins(Service.applications).findById(app.serviceNo).get
//
//      Application.deleteById(beforeService.applications.head.id)
//
//      val afterService = Service.joins(Service.applications).findById(app.serviceNo).get
//      afterService.applications.size should equal(
//        beforeService.applications.size - 1
//      )
//    }
//  }
}