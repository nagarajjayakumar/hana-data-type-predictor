package com.hortonworks.faas.spark.predictor.util

import java.sql.{Date, Timestamp}
import java.util

import org.apache.spark.sql.Row

/**
  * Created by njayakumar on 5/16/2018.
  */
object CommonData {
  private val FLOATING_EQUALITY_PRECISION: Double = 0.0000001d

  def getLength[T](x: Seq[T]): Int = {
    if (x != null) x.size else 0
  }

  def getLength[T](x: T): Int = {
    if (x != null) 1 else 0
  }

  /**
    * Check that all list are the same sizes (null are consider zero length)
    *
    * @param lists
    * @return
    */
  def checkListSizesMatch[T](lists: Seq[T]): Boolean = {
    lists.size match {
      case 0 => true
      case 1 => true
      case _ =>
        val tmp = lists.map(x => getLength(x)).aggregate[(Int, Int)]((Int.MaxValue, Int.MinValue))(
          (a, b) => (if (a._1 < b) a._1 else b, if (a._2 < b) b else a._2),
          (c, d) => (if (c._1 < d._1) c._1 else d._1, if (c._2 < d._2) d._2 else c._2))
        tmp._1 == tmp._2
    }
  }

  //  /**
  //    * Check that all arrays are the same sizes (null are consider zero length)
  //    *
  //    * @param arrays
  //    * @return
  //    */
  //  def checkListSizesMatch(arrays: Array[AnyRef]*): Boolean = {
  //    if ((arrays.length == 0) || (arrays.length == 1)) return true
  //    var baseSize: Int = 0
  //    if (arrays(0) != null) baseSize = arrays(0).length
  //    var index: Int = 1
  //    while (index < arrays.length) {
  //      {
  //        if (baseSize == 0) if ((arrays(index) != null) && (arrays(index).length != 0)) return false
  //        else if (arrays(index).length != baseSize) return false
  //      }
  //      {
  //        index += 1;
  //        index - 1
  //      }
  //    }
  //    true
  //  }

  val blankDouble: Double = Double.NaN

  val blankInteger: Int = Int.MinValue

  val blankLong:Long = Long.MinValue

  val blankString: String = null

  //  def blankDate: Date = Date.valueOf("1970-01-01")
  //  def blankTimestamp: Timestamp = Timestamp.valueOf("1970-01-01 00:00:00.0")

  val blankDate: Date = null

  val blankTimestamp: Timestamp = null

  val blankDecimal: BigDecimal = null

  def isBlank(value: Double): Boolean = {
    value.isNaN
  }

  def isNotBlank(value: Double): Boolean = !isBlank(value)

  def isBlank(value: Int): Boolean = {
    value == blankInteger
  }

  def isNotBlank(value: Int): Boolean = !isBlank(value)

  def isBlank(value: String): Boolean = {
    value == null || value.isEmpty || value.trim.equalsIgnoreCase("null")
  }

  def isNotBlank(value: String): Boolean = !isBlank(value)

  def isBlank(value: Date): Boolean = {
    value == null || value == blankDate
  }

  def isNotBlank(value: Date): Boolean = !isBlank(value)

  def isBlank(value: Timestamp): Boolean = {
    value == null || value == blankTimestamp
  }

  def isNotBlank(value: Timestamp): Boolean = !isBlank(value)

  def isEmptyOrNull(values: Array[Double]): Boolean = (values == null) || (values.length == 0)

  def isEmptyOrNull[T](values: util.Collection[T]): Boolean = (values == null) || values.isEmpty

  def isEmptyOrNull[K, V](values: Map[K, V]): Boolean = (values == null) || values.isEmpty

  def getIntOrBlank(r: Row, fidx: Int): Int = {
    if (r.isNullAt(fidx))
      CommonData.blankInteger
    else
      r.getInt(fidx)
  }

  def getIntOrBlank(r: Row, fieldName: String): Int = getIntOrBlank(r, r.fieldIndex(fieldName))

  def getLongOrBlank( r:Row, fidx:Int ) : Long = {
    if(r.isNullAt(fidx))
      blankLong
    else
      r.getLong(fidx)
  }

  def getLongOrBlank( r:Row, fieldName:String ) : Long = getLongOrBlank(r, r.fieldIndex(fieldName))

  def getDoubleOrBlank(r: Row, fidx: Int): Double = {
    if (r.isNullAt(fidx))
      CommonData.blankDouble
    else
      r.getDouble(fidx)
  }

  def getDoubleOrBlank(r: Row, fieldName: String): Double = getDoubleOrBlank(r, r.fieldIndex(fieldName))

  def getDateOrBlank(r: Row, fidx: Int): Date = {
    if (r.isNullAt(fidx))
      CommonData.blankDate
    else
      r.getDate(fidx)
  }

  def getDateOrBlank(r: Row, fieldName: String): Date = getDateOrBlank(r, r.fieldIndex(fieldName))

  def getStringOrBlank(r: Row, fidx: Int): String = {
    if (r.isNullAt(fidx))
      CommonData.blankString
    else {
      val tmp = r.getString(fidx)
      if( tmp.toLowerCase().trim() == "null")
        CommonData.blankString
      else
        tmp
    }
  }

  def getStringOrBlank(r: Row, fieldName: String): String = getStringOrBlank(r, r.fieldIndex(fieldName))

  def getTimestampOrBlank(r: Row, fidx: Int): Timestamp = {
    if (r.isNullAt(fidx))
      CommonData.blankTimestamp
    else
      r.getTimestamp(fidx)
  }

  def getTimestampOrBlank(r: Row, fieldName: String): Timestamp = getTimestampOrBlank(r, r.fieldIndex(fieldName))

  def getDecimalOrBlank(r: Row, fidx: Int): BigDecimal = {
    if (r.isNullAt(fidx))
      blankDecimal
    else
      r.getDecimal(fidx)
  }

  def getNumericOrBlank(r: Row, fieldName: String): BigDecimal = getDecimalOrBlank(r, r.fieldIndex(fieldName))

  def getDecimalOrBlank(r:Row, fieldName:String ) : BigDecimal = getDecimalOrBlank(r,r.fieldIndex(fieldName))

  def convertBlankToNull( v:Int ): Any  = {
    if( isBlank(v)) null else v
  }

  def convertBlankToNull( v:Double ) : Any = {
    if( isBlank(v)) null else v
  }

  def checkRange(value: Double, minimum: Option[Double], maximum: Option[Double]): Boolean = {
    if (isBlank(value)) false
    else if (minimum.isDefined && (value < minimum.get)) false
    else if (maximum.isDefined && (value > maximum.get)) false
    else true
  }

  def checkRange(value: Integer, minimum: Integer, maximum: Integer): Boolean = {
    if (isBlank(value)) return false
    if ((minimum != null) && (value < minimum)) return false
    if ((maximum != null) && (value > maximum)) return false
    true
  }

  def getBooleanFromString(value: String): Option[Boolean] = {
    if (value == null) None
    value.trim.toLowerCase match {
      case "true" =>
        Some(true)
      case "false" =>
        Some(false)
      case _ =>
        None
    }
  }

  def getBooleanFromStringForDBKeys(value: String): Boolean = {
    if (value == null) false
    value.trim.toLowerCase match {
      case "true" =>
        (true)
      case "false" =>
        (false)
      case _ =>
        false
    }
  }

  /**
    * This method is used to convert array of int type to array of double type. This method will return null if
    * originalArray is null.
    *
    * @param originalArray
    * array if int type
    * @return array of double type
    *
    */
  def convertIntArrayToDoubleArray(originalArray: Array[Int]): Array[Double] = {
    if (originalArray == null) return null

    originalArray.map(x => x.toDouble)

    //    if (originalArray == null) return null
    //    val convertedArray: Array[Double] = new Array[Double](originalArray.length)
    //    var count: Int = 0
    //    while (count < originalArray.length) {
    //      {
    //        convertedArray(count) = originalArray(count)
    //      }
    //      {
    //        count += 1;
    //        count - 1
    //      }
    //    }
    //    convertedArray
  }

  /**
    * This method is used to find elements from originalList based on index values in indexList and return the new
    * filteredList. If index list is empty or null original list is returned.This method assumes that the values in the
    * indexList will be greater than 0 and less than size of originalList else will throw IndexOutOfBoundsException.If
    * any of the index values are null, those will be removed.
    *
    * @param originalList
    * List of values on which filtering needs to be done
    * @param indexList
    * Index values which are used for filtering
    * @return List of elements which were present at the specified index values
    * @throws IndexOutOfBoundsException
    * if the index values are out of range
    */
  @throws[IndexOutOfBoundsException]
  def filterListByIndex[T](originalList: util.List[T], indexList: util.List[Integer]): util.List[T] = {
    if (isEmptyOrNull(indexList)) return originalList
    val filteredList: util.List[T] = new util.ArrayList[T](indexList.size)
    import scala.collection.JavaConversions._
    for (index <- indexList) {
      if (index != null) filteredList.add(originalList.get(index.intValue))
    }
    filteredList
  }

  /**
    * Convert List<Double> to double[]
    *
    * @param values
    * list of Double
    * @param skipNulls
    * true skip over any null values
    * @return list of primitive double
    */
  def convertToArray(values: List[Option[Double]], skipNulls: Boolean): Array[Option[Double]] = {
    values.filter(x => !skipNulls || x.isDefined).toArray
    //    var doubleArray: Array[Double] = null
    //    if (skipNulls) {
    //      var count: Int = 0
    //      import scala.collection.JavaConversions._
    //      for (value <- values) {
    //        if ((null != value) && !value.isNaN) count += 1
    //      }
    //      doubleArray = new Array[Double](count)
    //      count = 0
    //      import scala.collection.JavaConversions._
    //      for (value <- values) {
    //        if ((null != value) && !value.isNaN) doubleArray({
    //          count += 1;
    //          count - 1
    //        }) = value.doubleValue
    //      }
    //    }
    //    else {
    //      doubleArray = new Array[Double](values.size)
    //      var count: Int = 0
    //      import scala.collection.JavaConversions._
    //      for (value <- values) {
    //        doubleArray({
    //          count += 1;
    //          count - 1
    //        }) = value.doubleValue
    //      }
    //    }
    //    doubleArray
  }

  /**
    * Generates sequence of values starting and ending on given values, with two consecutive values separated by given
    * increment
    *
    * @param startValue
    * Starting value for sequence
    * @param endValue
    * Ending value for sequence
    * @param increment
    * Difference between two consecutive values in sequence
    * @return Sequence of values in the form of list
    */
  def generateSequence(startValue: Int, endValue: Int, increment: Int): util.List[Integer] = {
    if (endValue < startValue) throw new IllegalArgumentException("Start Value = " + startValue + ", End Value = " + endValue + ".End value must be greator than or equal to start value")
    val sequenceList: util.List[Integer] = new util.ArrayList[Integer]
    sequenceList.add(startValue)
    var sequenceVal: Int = startValue + increment
    while (sequenceVal <= endValue) {
      sequenceList.add(sequenceVal)
      sequenceVal += increment
    }
    sequenceList
  }

  /**
    * Convert a String to an Integer. If the conversion fails, returning a default value.
    *
    * @param stringValue
    * the string to convert, may be null
    * @param defaultValue
    * the default value
    * @return the Integer represented by the string, or the default if conversion fails
    */
  def parseInt(stringValue: String, defaultValue: Integer): Integer = {
    try {
      Integer.parseInt(stringValue)
    } catch {
      case e: NumberFormatException =>
        defaultValue
    }
  }


  /**
    * Compare two floating values
    *
    * @param firstValue
    * the first comparing value
    * @param secondValue
    * the second comparing value
    * @return if the absolute difference below precision return true, else return false
    */
  def floatingValueEquals(firstValue: Double, secondValue: Double): Boolean = Math.abs(firstValue - secondValue) < FLOATING_EQUALITY_PRECISION
}


