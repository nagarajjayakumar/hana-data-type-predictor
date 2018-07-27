package com.hortonworks.faas.spark.predictor.mdb.persistor

import com.hortonworks.faas.spark.predictor.mdb.common.MetaDataBaseOptions
import com.hortonworks.faas.spark.predictor.mdb.model.SourceDbActiveObjectDetail
import com.hortonworks.faas.spark.predictor.orm.service.Connection
import com.hortonworks.faas.spark.predictor.orm.setting.DBSettings
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import scalikejdbc.{DB, NamedDB}

class hana_metadata_details_persistor(val spark: SparkSession,
                                      val mdpopts: MetaDataBaseOptions,
                                      saod: List[SourceDbActiveObjectDetail],
                                      schemaMap: Map[String, StructType]) extends Logging with DBSettings with Connection {

  // initialize logger
  log

  def isValid(): Boolean = {
    true
  }

  // Following env and db is for the metadata
  def env(): String = mdpopts.mdbenvironment

  // Following env and db is for the metadata
  def db(): DB = NamedDB(mdpopts.mdbservice).toDB()

  def updateDbActiveObjectDetails(): Unit = {

    val sourceSchema: StructType = schemaMap.get("originalSchema").get
    logInfo(s"sourceSchema --> ${sourceSchema} " )
    val inferSchema: StructType  = schemaMap.get("inferSchema").get
    logInfo(s"inferSchema --> ${inferSchema} " )

    for((dboadetail, index) <- saod.zipWithIndex){
      val colSourceStructType :StructType = StructType(sourceSchema.fields.filter(f => dboadetail.columnName.equalsIgnoreCase(f.name)))
      logDebug("colSourceStructType " + colSourceStructType)

      val colInferStructType :StructType = StructType(inferSchema.fields.filter(f => dboadetail.columnName.equalsIgnoreCase(f.name)))
      logDebug("colInferStructType " + colInferStructType)

      var sourceDataType = "null"
      if(colSourceStructType.fields.nonEmpty){
        sourceDataType = colSourceStructType.fields.head.dataType.simpleString
      }

      var inferDataType = "null"
      if(colSourceStructType.fields.nonEmpty){
        inferDataType = colInferStructType.fields.head.dataType.simpleString
      }

      SourceDbActiveObjectDetail.updateById(dboadetail.id).
          withAttributes('sourceDataType -> sourceDataType,
            'inferDataType -> inferDataType)

    }
    logDebug(s"Updated the active object details hao id -> ${saod.head.haoid} with source and infer data type ")

  }

}

object hana_metadata_details_persistor {

  def apply( spark: SparkSession,
             mdpopts: MetaDataBaseOptions,
             saod: List[SourceDbActiveObjectDetail],
             schemaMap: Map[String, StructType]): hana_metadata_details_persistor = {
    new hana_metadata_details_persistor( spark, mdpopts, saod,schemaMap)
  }


}
