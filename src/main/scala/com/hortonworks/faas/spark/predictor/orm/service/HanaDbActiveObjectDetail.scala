package com.hortonworks.faas.spark.predictor.orm.service

import org.joda.time.DateTime
import scalikejdbc._
import skinny.orm.SkinnyCRUDMapper
import skinny.orm.feature.{SoftDeleteWithTimestampFeature, TimestampsFeature}

//// id="SAPClient" key="true" order="1" attributeHierarchyActive="false" displayAttribute="false"
case class HanaDbActiveObjectDetail(
                               id: Long,
                               haoid: Long,
                               columnName: String,
                               isKey: Boolean,
                               col_order: Int,
                               attributeHierarchyActive: Boolean,
                               displayAttribute: Boolean,
                               defaultDescription: String,
                               sourceObjectName: String,
                               sourceColumnName: String,
                               isRequiredForFlow: Boolean,
                               createdAt: DateTime,
                               updatedAt: DateTime
                             )

object HanaDbActiveObjectDetail
  extends SkinnyCRUDMapper[HanaDbActiveObjectDetail]
    with TimestampsFeature[HanaDbActiveObjectDetail]
    with SoftDeleteWithTimestampFeature[HanaDbActiveObjectDetail] {

  override val connectionPoolName = 'service
  override val tableName = "hanadb_active_object_detail"
  override def primaryKeyFieldName = "id"

  override def defaultAlias = createAlias("haod")

  override def extract(rs: WrappedResultSet, n: ResultName[HanaDbActiveObjectDetail]) = autoConstruct(rs, n)


}