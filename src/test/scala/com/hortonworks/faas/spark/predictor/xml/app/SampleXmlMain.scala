package com.hortonworks.faas.spark.predictor.xml.app

import com.hortonworks.faas.spark.predictor.xml.models.{Catalog, ItemSize}
import com.hortonworks.faas.spark.predictor.xml.parser.XmlParser
import com.hortonworks.faas.spark.predictor.xml.validator._

import scala.xml._


/**
  * Testing conversion
  */
object CatalogMain extends App {

  xmlTest()
  validationTest()

  def xmlTest(): Unit = {
    val xmlFile = this.getClass.getResource("/sample-test.xml")
    val xmlSource = XML.load(xmlFile.getPath)

    val catalog = XmlParser.parse(xmlSource)(Catalog.xmlRead)

    catalog.foreach(println)

    val newImg = XmlParser.parse(xmlSource)(ItemSize.xmlRead)

    newImg.foreach(println)
  }

  def validationTest(): Unit = {
    val usr = User("bat", "mat", 2)
    println("==============FIRST USER VALIDATION==============")
    usr.validate().foreach(println)

    val usr2 = User("usr_idds", "haa", 10, Some("homerr"))
    println("==============SECOND USER VALIDATION==============")
    usr2.validate().foreach(println)
  }
}

case class User(
                 user_id: String,
                 first_name: String,
                 age: Int,
                 group: Option[String] = None
               ) {

  import com.hortonworks.faas.spark.predictor.xml.validator.DefaultValidations._

  private val validationsNew: Seq[FieldValidator[_]] = Seq(
    StringField("user_id", user_id,
      StartsWith("usr") and Length(min = 5)
    ),
    IntField("age", age,
      InBetween(min = 2)
    ),
    OptField[String]("group", group,
      StartsWith("aa") and Length(max = 4, min = 2)
    )
  )

  def validate(group: Option[String] = None): Seq[String] = validationsNew.flatMap(_.validate(group))
}
