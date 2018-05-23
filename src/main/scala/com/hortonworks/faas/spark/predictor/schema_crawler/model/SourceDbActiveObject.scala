package com.hortonworks.faas.spark.predictor.schema_crawler.model

import org.joda.time.DateTime
import scalikejdbc._
import skinny.orm.SkinnyCRUDMapper
import skinny.orm.feature.TimestampsFeature

case class SourceDbActiveObject(
                                 id: Long,
                                 namespace: String,
                                 dbObjectName: String,
                                 dbObjectType: String,
                                 dbObjectTypeSuffix: String,
                                 version: Int,
                                 activatedAt: DateTime,
                                 activatedBy: String,
                                 isActive: Boolean = true,
                                 hanaDbActiveObjectDetails: Seq[SourceDbActiveObjectDetail] = Nil,
                                 createdAt: DateTime,
                                 updatedAt: DateTime
                             )

object SourceDbActiveObject
  extends SkinnyCRUDMapper[SourceDbActiveObject]
    with TimestampsFeature[SourceDbActiveObject] {

  override val connectionPoolName = 'service
  override val tableName = "hanadb_active_object"

  override def primaryKeyFieldName = "id"

  override def defaultAlias = createAlias("hao")


  override def extract(rs: WrappedResultSet, n: ResultName[SourceDbActiveObject]) = autoConstruct(rs, n, "hanaDbActiveObjectDetails")

  val hanaDbActiveObjectDetails = hasMany[SourceDbActiveObjectDetail](
    many = SourceDbActiveObjectDetail -> SourceDbActiveObjectDetail.defaultAlias,
    on = (hao, haod) => sqls.eq(hao.id, haod.haoid),
    merge = (hao, hanaDbActiveObjectDetails) => hao.copy(hanaDbActiveObjectDetails = hanaDbActiveObjectDetails)
  )


}
