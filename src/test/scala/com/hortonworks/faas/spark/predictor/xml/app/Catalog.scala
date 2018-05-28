package com.hortonworks.faas.spark.predictor.xml.app

import com.hortonworks.faas.spark.predictor.xml.parser.{XmlNodeReader, XmlNodeWriter, XmlParser}


/**
  * Created by naga jay on 5/05/18.
  *
  * Classes for testing xml
  */

case class Catalog(products: Seq[CatalogProduct])

object Catalog {
  val xmlRead = new XmlNodeReader[Catalog](
    nodeName = "catalog", prefixName = Some(""),
    read = n => {
      val products = XmlParser.parse(n)(CatalogProduct.xmlRead)

      Catalog(products)
    }
  )
}

case class CatalogProduct(description: String, productImage: String, catalogItems: Seq[CatalogItem])

object CatalogProduct {
  val xmlRead = new XmlNodeReader[CatalogProduct](
    nodeName = "product", prefixName = Some(""),
    read = n => {
      val items = XmlParser.parse(n)(CatalogItem.xmlRead)

      CatalogProduct(
        (n \ "@description").text,
        (n \ "@product_image").text,
        items
      )
    }
  )
}

case class CatalogItem(gender: String, itemNumber: String, price: Float, size: Seq[ItemSize])

object CatalogItem {
  val xmlRead = new XmlNodeReader[CatalogItem](
    nodeName = "catalog_item", prefixName = Some(""),
    read = n => {
      val sizes = XmlParser.parse(n)(ItemSize.xmlRead)
      val price = try {
        (n \ "price").text.toFloat
      } catch {
        case _: Exception => 0.0F
      }
      CatalogItem(
        (n \ "@gender").text,
        (n \ "item_number").text,
        price,
        sizes
      )
    }
  )
}

case class ItemSize(description: String, images: Seq[ItemImage])

object ItemSize {
  val xmlRead = new XmlNodeReader[ItemSize](
    nodeName = "size", prefixName = Some(""),
    read = n => {
      val images = XmlParser.parse(n)(ItemImage.xmlRead)
      ItemSize(
        (n \ "@description").text,
        images
      )
    }
  )

  val xmlWriter = new XmlNodeWriter[ItemSize](
    write = i => {
      <size description={i.description}>
        {XmlParser.write(i.images)(ItemImage.xmlWriter)}
      </size>
    }
  )
}

case class ItemImage(image: String, color: String)

object ItemImage {
  val xmlRead = new XmlNodeReader[ItemImage](
    nodeName = "color_swatch", prefixName = Some(""),
    read = n => {
      ItemImage(
        (n \ "@image").text,
        n.text
      )
    }
  )

  val xmlWriter = new XmlNodeWriter[ItemImage](
    write = i => {
      <color_swatch image={i.image}>
        {i.color}
      </color_swatch>
    }
  )
}
