package com.hortonworks.faas.spark.predictor.orm.service


import org.joda.time.DateTime
import scalikejdbc._
import skinny.orm.SkinnyCRUDMapper
import skinny.orm.feature.{ TimestampsFeature}

case class HanaDbActiveObject(
                               id: Long,
                               namespace: String,
                               dbObjectName: String,
                               dbObjectType: String,
                               dbObjectTypeSuffix: String,
                               version: Int,
                               activatedAt: DateTime,
                               activatedBy: String,
                               isActive: Boolean = true,
                               hanaDbActiveObjectDetails: Seq[HanaDbActiveObjectDetail] = Nil,
                               createdAt: DateTime,
                               updatedAt: DateTime
                             )

object HanaDbActiveObject
  extends SkinnyCRUDMapper[HanaDbActiveObject]
    with TimestampsFeature[HanaDbActiveObject]
    {

  override val connectionPoolName = 'service
  override val tableName = "hanadb_active_object"

  override def primaryKeyFieldName = "id"
  override def defaultAlias = createAlias("hao")


  override def extract(rs: WrappedResultSet, n: ResultName[HanaDbActiveObject]) = autoConstruct(rs, n, "hanaDbActiveObjectDetails")

  val hanaDbActiveObjectDetails = hasMany[HanaDbActiveObjectDetail](
    many = HanaDbActiveObjectDetail -> HanaDbActiveObjectDetail.defaultAlias,
    on = (hao, haod) => sqls.eq(hao.id, haod.haoid),
    merge = (hao, hanaDbActiveObjectDetails) => hao.copy(hanaDbActiveObjectDetails = hanaDbActiveObjectDetails)
  )


}
