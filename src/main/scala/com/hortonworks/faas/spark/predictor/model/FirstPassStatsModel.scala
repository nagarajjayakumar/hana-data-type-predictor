package com.hortonworks.faas.spark.predictor.model


import scala.collection.mutable

class FirstPassStatsModel extends Serializable {
  var columnStatsMap = new mutable.HashMap[Integer, ColumnStats]

  def +=(colIndex: Int, colValue: Any, colCount: Long): Unit = {
    columnStatsMap.getOrElseUpdate(colIndex, new ColumnStats) += (colValue, colCount)
  }

  def +=(firstPassStatsModel: FirstPassStatsModel): Unit = {
    firstPassStatsModel.columnStatsMap.foreach{ e =>
      val columnStats = columnStatsMap.getOrElse(e._1, null)
      if (columnStats != null) {
        columnStats += (e._2)
      } else {
        columnStatsMap += ((e._1, e._2))
      }
    }
  }

  override def toString = s"FirstPassStatsModel(columnStatsMap=$columnStatsMap)"
}
