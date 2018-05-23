package com.hortonworks.faas.spark.predictor.schema_crawler.model

import org.joda.time.DateTime
import scalikejdbc._
import skinny.orm.SkinnyCRUDMapper
import skinny.orm.feature.TimestampsFeature

case class SourceDbActiveObjectDetail(
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

object SourceDbActiveObjectDetail
  extends SkinnyCRUDMapper[SourceDbActiveObjectDetail]
    with TimestampsFeature[SourceDbActiveObjectDetail] {

  override val connectionPoolName = 'service
  override val tableName = "hanadb_active_object_detail"

  override def primaryKeyFieldName = "id"

  override def defaultAlias = createAlias("haod")

  override def extract(rs: WrappedResultSet, n: ResultName[SourceDbActiveObjectDetail]) = autoConstruct(rs, n)


}