package com.hortonworks.faas.spark.predictor.xml.models

import com.hortonworks.faas.spark.predictor.xml.parser.{XmlNodeReader, XmlNodeWriter, XmlParser}


/**
  * Created by naga jay on 5/05/18.
  *
  * Classes for testing xml
  */

case class Calculation(products: Seq[LogicalModel])

object Calculation
{
  val xmlRead = new XmlNodeReader[Calculation](
    nodeName = "scenario", prefixName = Some("Calculation"),
    read = n => {
      val logicalModel = XmlParser.parse(n)(LogicalModel.xmlRead)
      Calculation(logicalModel)
    }
  )
}

case class LogicalModel(attributeItems: Seq[LogicalModelAttributes])

object LogicalModel
{
  val xmlRead = new XmlNodeReader[LogicalModel](
    nodeName = "logicalModel", prefixName = Some (""),
    read = n => {
      val logicalModelAttributes = XmlParser.parse(n)(LogicalModelAttributes.xmlRead)

      LogicalModel(logicalModelAttributes)
    }
  )
}

case class LogicalModelAttributes( logicalModelAttribute: Seq[LogicalModelAttribute])

object LogicalModelAttributes
{
  val xmlRead = new XmlNodeReader[LogicalModelAttributes](
    nodeName = "attributes",
    read = n => {
      val attributes = XmlParser.parse(n)(LogicalModelAttribute.xmlRead)

      LogicalModelAttributes(attributes)
    }
  )
}

// id="SAPClient" key="true" order="1" attributeHierarchyActive="false" displayAttribute="false"
case class LogicalModelAttribute(id: String, key: String, order: String ,
                                 attributeHierarchyActive: String, displayAttribute: String,
                                 logicalModelAttributesAttribDesc: Seq[LogicalModelAttributesAttribDesc],
                                 logicalModelAttributesAttribKeyMapping: Seq[LogicalModelAttributesAttribKeyMapping]
                                )

object LogicalModelAttribute
{
  val xmlRead = new XmlNodeReader[LogicalModelAttribute](
    nodeName = "attribute",
    read = n => {
      val logicalModelAttributesAttribDesc = XmlParser.parse(n)(LogicalModelAttributesAttribDesc.xmlRead)
      val logicalModelAttributesAttribKeyMapping = XmlParser.parse(n)(LogicalModelAttributesAttribKeyMapping.xmlRead)


      LogicalModelAttribute(
        (n \ "@id").text,
        (n \ "@key").text,
        (n \ "@order").text,
        (n \ "@attributeHierarchyActive").text,
        (n \ "@displayAttribute").text,
        logicalModelAttributesAttribDesc, logicalModelAttributesAttribKeyMapping
      )
    }
  )

  val xmlWriter = new XmlNodeWriter[LogicalModelAttribute](
    write = i => {
      <attribute id={i.id} key={i.key} order={i.order} attributeHierarchyActive={i.attributeHierarchyActive} displayAttribute={i.displayAttribute} >
        {XmlParser.write(i.logicalModelAttributesAttribDesc)(LogicalModelAttributesAttribDesc.xmlWriter)}
        {XmlParser.write(i.logicalModelAttributesAttribKeyMapping)(LogicalModelAttributesAttribKeyMapping.xmlWriter)}
      </attribute>
    }
  )
}

case class LogicalModelAttributesAttribDesc(defaultDescription: String)

object LogicalModelAttributesAttribDesc
{
  val xmlRead = new XmlNodeReader[LogicalModelAttributesAttribDesc](
    nodeName = "descriptions",
    read = n => {
      LogicalModelAttributesAttribDesc((n \ "@defaultDescription").text )
    }
  )

  val xmlWriter = new XmlNodeWriter[LogicalModelAttributesAttribDesc](
    write = i => {
      <descriptions defaultDescription={i.defaultDescription} />

    }
  )
}

case class LogicalModelAttributesAttribKeyMapping(columnObjectName: String, columnName: String)

object LogicalModelAttributesAttribKeyMapping
{
  val xmlRead = new XmlNodeReader[LogicalModelAttributesAttribKeyMapping](
    nodeName = "keyMapping",
    read = n => {
      LogicalModelAttributesAttribKeyMapping((n \ "@columnObjectName").text , (n \ "@columnName").text )
    }
  )

  val xmlWriter = new XmlNodeWriter[LogicalModelAttributesAttribKeyMapping](
    write = i => {
      <keyMapping columnObjectName={i.columnObjectName} columnName={i.columnName} />
    }
  )
}
