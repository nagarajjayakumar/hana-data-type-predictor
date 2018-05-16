package com.hortonworks.faas.spark.predictor.inference_engine.analytic.common

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by njayakumar on 5/16/2018.
  */
object CommonDataFrameWriter {
  def write(df: DataFrame, cdfw: CommonDataFrameWriterOption): Unit = {
    write(df, cdfw.write_mode, cdfw.output)
  }

  def write(df: DataFrame, write_mode: String, path: String): Unit = {
    write_mode.toLowerCase match {
      case "orc" =>
        writeORC(df, path)
      case "csv" =>
        writeCSV(df, path)
      case _ =>
        writeJSON(df, path)
    }
  }

  def writeCSV(df: DataFrame, path: String): Unit = {
    df.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).save(path)
  }

  def writeJSON(df: DataFrame, path: String): Unit = {
    df.write.mode(SaveMode.Overwrite).json(path)
  }

  def writeORC(df: DataFrame, path: String): Unit = {
    df.write.mode(SaveMode.Overwrite).orc(path)
  }
}
