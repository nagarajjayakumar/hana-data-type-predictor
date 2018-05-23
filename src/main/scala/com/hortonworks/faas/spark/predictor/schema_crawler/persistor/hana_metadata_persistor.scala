package com.hortonworks.faas.spark.predictor.schema_crawler.persistor

import com.hortonworks.faas.spark.predictor.schema_crawler.model.HanaActiveObject
import com.hortonworks.faas.spark.predictor.util.Logging
import org.apache.spark.sql.Dataset

object hana_metadata_persistor extends Logging{


  def persist(df: Dataset[HanaActiveObject]): Unit = {

  }

}
