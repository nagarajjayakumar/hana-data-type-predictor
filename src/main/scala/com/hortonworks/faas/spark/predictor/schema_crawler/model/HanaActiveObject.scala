package com.hortonworks.faas.spark.predictor.schema_crawler.model

case class HanaActiveObject (
    PACKAGE_ID: Option[String],
    OBJECT_NAME: Option[String],
    OBJECT_SUFFIX: Option[String],
    VERSION_ID: Option[Int],
    ACTIVATED_AT: Option[java.sql.Timestamp],
    ACTIVATED_BY: Option[String],
    EDIT: Option[Int],
    CDATA: Option[String],
    BDATA: Option[Array[Byte]],
    COMPRESSION_TYPE: Option[Int],
    FORMAT_VERSION: Option[String],
    DELIVERY_UNIT: Option[String],
    DU_VERSION: Option[String],
    DU_VENDOR: Option[String],
    DU_VERSION_SP: Option[String],
    DU_VERSION_PATCH: Option[String],
    OBJECT_STATUS: Option[Int],
    CHANGE_NUMBER: Option[Int],
    RELEASED_AT: Option[java.sql.Timestamp]
) extends DbActiveObject
