package com.hortonworks.faas.spark.predictor.util

import java.io.InputStream
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SparkSession

/**
  * Small utility trait to make it easier to work with Hadoop FileSystems. Does not close the filesystem since it seems to be cleaned up via a shutdown hook
  * and closing it manually causes a warning and some problems.
  */
trait DfsUtils {
  def copyToLocalFile(spark: SparkSession,
                      fsBase: URI,
                      source: Path,
                      target: Path): Unit = {
    val fs = FileSystem.get(fsBase, spark.sparkContext.hadoopConfiguration)
    fs.copyToLocalFile(source, target)
  }

  def copyFromLocalFile(spark: SparkSession,
                        fsBase: URI,
                        source: Path,
                        target: Path,
                        deleteTargetOnExit: Boolean = false): Unit = {
    val fs = FileSystem.get(fsBase, spark.sparkContext.hadoopConfiguration)
    fs.copyFromLocalFile(source, target)
    if (deleteTargetOnExit) {
      fs.deleteOnExit(target)
    }
  }

  def copyFromStream(spark: SparkSession,
                     fsBase: URI,
                     source: InputStream,
                     target: Path): Unit = {
    val fs = FileSystem.get(fsBase, spark.sparkContext.hadoopConfiguration)
    val out = fs.create(target)

    IOUtils.copyBytes(source, out, spark.sparkContext.hadoopConfiguration)
  }
}
