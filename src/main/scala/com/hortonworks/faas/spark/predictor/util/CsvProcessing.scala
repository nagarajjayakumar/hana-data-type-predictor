package com.hortonworks.faas.spark.predictor.util

import java.sql.{Date, Timestamp}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, Month}

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait CsvProcessing extends ExecutionTiming {
  def fixSchemaViaSelect(schema: StructType, input: DataFrame): DataFrame = {
    var result = input

    for (f <- schema.fields) {
      //SAS Dates are typed as doubles and need special handling to convert
      if (f.name.toLowerCase.contains("date")) {
        if (f.dataType == DoubleType) {
          result = result.withColumn(f.name, convertSasDateUdf(result.col(f.name)))
        } else if (f.dataType == IntegerType) {
          result = result.withColumn(f.name, convertIntegerDateUdf(result.col(f.name)))
        } else if (f.dataType == StringType) {
          result = result.withColumn(f.name, convertStringDateUdf(result.col(f.name)))
        }
      } else if (f.name.toLowerCase.endsWith("dtc")) {
        //these are usually stored as a SAS date string when coming from SAS
        if (f.dataType == StringType) {
          result = result.withColumn(f.name, convertStringTimestampUdf(result.col(f.name)))
        }
      }
    }

    result
  }

  /**
    * Convert a date of the given format stored as an integer in the yyyymmdd format to a java.sql.Date type.
    */
  private def convertIntegerDateUdf = udf(convertIntegerDate)

  protected def convertIntegerDate: (Integer) => Date = (input: Integer) => {
    try {
      if(input != null) {
        Date.valueOf(LocalDate.parse(input.toString, DateTimeFormatter.BASIC_ISO_DATE))
      } else {
        null
      }
    } catch {
      case e: DateTimeParseException => null
    }
  }

  /**
    * UDF function to convert the SAS Date to a java.sql.Date
    *
    * SAS Dates are a really strange format, see http://support.sas.com/documentation/cdl/en/lrcon/65287/HTML/default/viewer.htm#p1wj0wt2ebe2a0n1lv4lem9hdc0v.htm for more information.
    */
  private def convertSasDateUdf = udf(convertSasDate)

  protected def convertSasDate: (Double) => Timestamp = (input: Double) => {
    //Note: Trying to set the base date in a constant caused a bunch of serialization errors
    Timestamp.valueOf(LocalDateTime.of(1960, Month.JANUARY, 1, 0, 0).plus(input.toInt, ChronoUnit.DAYS))
  }

  /**
    * UDF function to convert a text date into a java.sql.Date
    */
  private def convertStringDateUdf = udf(convertStringDate)

  protected def convertStringDate: (String) => Date = (input: String) => {
    try {
      if (input == null) {
        null
      } else {
        Date.valueOf(LocalDate.parse(input, DateTimeFormatter.ISO_LOCAL_DATE))
      }
    } catch {
      case e: DateTimeParseException => null
    }
  }

  /**
    * UDF function to convert a text timestamp into a java.sql.Timestamp
    */
  private def convertStringTimestampUdf = udf(convertStringTimestamp)

  protected def convertStringTimestamp: (String) => Timestamp = (input: String) => {
    try {
      if (input == null) {
        null
      }
      else if (input.matches("\\d{4}-\\d{2}-\\d{2}")) {
        Timestamp.valueOf(LocalDateTime.parse(input + "T00:00", DateTimeFormatter.ISO_LOCAL_DATE_TIME))
      } else {
        Timestamp.valueOf(LocalDateTime.parse(input, DateTimeFormatter.ISO_LOCAL_DATE_TIME))
      }
    } catch {
      case e: DateTimeParseException => null
    }
  }

  def saveCsvToTable(spark: SparkSession, csvPath: String, schema: String, tableName: String, delimiter: String = ","): Unit = {
    //TODO: look to see if there is a way to load the precursor result for both initial and final into a DF or RDD for caching
    val qualifiedTableName = s"${schema}.${tableName}"
    time(s"Dropping existing table ${qualifiedTableName}", {
      spark.sql(s"DROP TABLE IF EXISTS ${qualifiedTableName}")
    })

    val initialCsv = time(s"Scanning to derive initial schema - ${csvPath}", {
      spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", delimiter).csv(csvPath)
    })
    initialCsv.cache()

    val finalCsv = time(s"Loading with modified schema - ${csvPath}", {
      fixSchemaViaSelect(initialCsv.schema, initialCsv)
    })

    println("PRINTING SCHEMA")
    finalCsv.printSchema()

    time(s"Saving final table to hive - ${qualifiedTableName}", {
      finalCsv.write.saveAsTable(schema + "." + tableName)
    })

    initialCsv.unpersist()
  }
}
