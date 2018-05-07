package com.hortonworks.faas.spark.predictor.xml.parser

import scala.xml.{Node, NodeSeq}

/**
  * Created by naga jay on 5/05/18.
  * Object containing xml parsing function
  *
  */

object XmlParser
{

  /**
    * Converts xml Node to Sequence of T
    *
    * @param xmlInput
    * @param reader
    * @tparam T
    *
    * @return
    */
  def parse[T](xmlInput: Node)(reader: XmlNodeReader[T]): Seq[T] =
  {
    reader.readTo(xmlInput)
  }

  /**
    * Converts sequence of T to sequence of Node
    *
    * @param in
    * @param writer
    * @tparam T
    *
    * @return
    */
  def write[T](in: Seq[T])(writer: XmlNodeWriter[T]): Seq[Node] =
  {
    NodeSeq.fromSeq(in.map(i => XmlParser.write[T](i)(writer)))
  }

  /**
    * Converts To to xml Node
    *
    * @param in
    * @param writer
    * @tparam T
    *
    * @return
    */
  def write[T](in: T)(writer: XmlNodeWriter[T]): Node =
  {
    writer.convertTo(in)
  }
}

/**
  * Contract to convert from FT to T
  *
  * @tparam FT
  * @tparam T
  */
trait ConvertTo[FT, T]
{
  /**
    * method that performs the conversion
    *
    * @param xml
    *
    * @return
    */
  def convertTo(xml: FT): T
}

/**
  * Converts xml Node to type T
  *
  * @param nodeName
  * @param read
  * @tparam T
  */
class XmlNodeReader[T](
  nodeName: String,
  read: (Node => T)
) extends ConvertTo[Node, T]
{
  def convertTo(xml: Node): T =
  {
    read(xml)
  }

  def readTo(xml: Node, overrideNodeName: String = nodeName): Seq[T] =
  {
    (xml \\ overrideNodeName).map(convertTo)
  }
}

/**
  * Converts type T to xml Node
  *
  * @param write
  * @tparam T
  */
class XmlNodeWriter[T](
  write: (T => Node)
) extends ConvertTo[T, Node]
{
  def convertTo(in: T): Node = write(in)
}