package com.hortonworks.faas.spark.predictor.xml.app

import com.hortonworks.faas.spark.predictor.xml.models.{Calculation, LogicalModelAttribute}
import com.hortonworks.faas.spark.predictor.xml.parser.XmlParser

import scala.xml._


/**
  * Testing conversion
  */
object Main extends App {

  xmlTest()


  def xmlTest(): Unit = {
    val xmlFile = this.getClass.getResource("/hana-metadata-sample.xml")
    val xmlSource = XML.load(xmlFile.getPath)

    val calculation = XmlParser.parse(xmlSource)(Calculation.xmlRead)

    calculation.foreach(println)

    val logicalModelAttribute = XmlParser.parse(xmlSource)(LogicalModelAttribute.xmlRead)

    logicalModelAttribute.foreach(println)
  }


}

