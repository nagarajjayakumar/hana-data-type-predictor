package com.hortonworks.faas.spark.predictor.xml.validator

/**
  * Class to perform field validation for class properties
  *
  * @param fieldName   : name of the property
  * @param fieldValue  : value of the property to validate
  * @param validations : set of validation to run against fieldValue
  * @tparam T : Type for the validator classes
  */
class ClassFieldValidator[T](
                              val fieldName: String,
                              val fieldValue: T,
                              val validations: ValidationContainer[T]
                            ) extends FieldValidator[T] {
  def validate(group: Option[String] = None): Seq[String] = {
    var errorMessage = scala.collection.mutable.ListBuffer[String]()

    validations.validations.foreach { fieldValidation =>
      val inValidationGroup = group match {
        case Some(s: String) => fieldValidation.groups.contains(s)
        case _ => true
      }

      if (inValidationGroup && !fieldValidation.validate(fieldValue)) {
        errorMessage += fieldValidation.getError(fieldName)
      }
    }

    errorMessage
  }
}

object ClassFieldValidator {
  /**
    * Implicit apply to convert tuple to ClassFieldValidator
    *
    * @param tuple : tuple to convert to
    * @tparam T : Type for the validator classes
    * @return
    */
  implicit def apply[T](tuple: (String, T, ValidationContainer[T])): ClassFieldValidator[T] = ClassFieldValidator(tuple._1, tuple._2, tuple._3)
}

/**
  * Int field validator
  *
  * @param fieldName   : name of the property
  * @param fieldValue  : value of the property to validate
  * @param validations : set of validation to run against fieldValue
  */
case class IntField(
                     override val fieldName: String,
                     override val fieldValue: Int,
                     override val validations: ValidationContainer[Int]
                   ) extends ClassFieldValidator[Int](
  fieldName,
  fieldValue,
  validations
)

/**
  * String field validator
  *
  * @param fieldName   : name of the property
  * @param fieldValue  : value of the property to validate
  * @param validations : set of validation to run against fieldValue
  */
case class StringField(
                        override val fieldName: String,
                        override val fieldValue: String,
                        override val validations: ValidationContainer[String]
                      ) extends ClassFieldValidator[String](
  fieldName,
  fieldValue,
  validations
)

/**
  * Boolean field validator
  *
  * @param fieldName   : name of the property
  * @param fieldValue  : value of the property to validate
  * @param validations : set of validation to run against fieldValue
  */
case class BooleanField(
                         override val fieldName: String,
                         override val fieldValue: Boolean,
                         override val validations: ValidationContainer[Boolean]
                       ) extends ClassFieldValidator[Boolean](
  fieldName,
  fieldValue,
  validations
)

/**
  * Float field validator
  *
  * @param fieldName   : name of the property
  * @param fieldValue  : value of the property to validate
  * @param validations : set of validation to run against fieldValue
  */
case class FloatField(
                       override val fieldName: String,
                       override val fieldValue: Float,
                       override val validations: ValidationContainer[Float]
                     ) extends ClassFieldValidator[Float](
  fieldName,
  fieldValue,
  validations
)

/**
  * Double field validator
  *
  * @param fieldName   : name of the property
  * @param fieldValue  : value of the property to validate
  * @param validations : set of validation to run against fieldValue
  */
case class DoubleField(
                        override val fieldName: String,
                        override val fieldValue: Double,
                        override val validations: ValidationContainer[Double]
                      ) extends ClassFieldValidator[Double](
  fieldName,
  fieldValue,
  validations
)

/**
  * Optional field validator
  *
  * @param fieldName   : name of the property
  * @param fieldValue  : value of the property to validate
  * @param validations : set of validation to run against fieldValue
  * @tparam T : Type for the validator classes
  */
case class OptField[T](
                        override val fieldName: String,
                        fieldValue: Option[T],
                        override val validations: ValidationContainer[T]
                      ) extends FieldValidator[T] {
  override def validate(group: Option[String]): Seq[String] = {
    //validate only if the value is present
    if (fieldValue == null || fieldValue.isEmpty) Seq.empty
    else {
      val validator = new ClassFieldValidator(
        fieldName,
        fieldValue.get,
        validations
      )
      validator.validate(group)
    }
  }
}