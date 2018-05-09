package com.hortonworks.faas.spark.predictor.xml.validator

/**
  * trait that defines the property/behaviour of the validator classes
  *
  * @tparam T : Type for the validator classes
  */
trait Validate[T] {
  /**
    * Validation Groups current validation is related to
    */
  val groups: Seq[String]

  /**
    * Main method that performs validation
    *
    * @param checkVal : value to check for validation
    * @return
    */
  def validate(checkVal: T): Boolean

  /**
    * Get the error if the validation has failed
    * This will always return error validation for each validator and is not tied to validation success/fail
    *
    * @param field : field used for validation
    * @return
    */
  def getError(field: String): String

  /**
    * Connector method to generate ValidationContainer and keep on connecting Validations using 'and' method to
    * generate seq of validation inside ValidationContainer
    *
    * @param newValidation : Validation to be added to list
    * @return
    */
  def and(newValidation: Validate[T]): ValidationContainer[T] = {
    val combinedSequence: Seq[Validate[T]] = Seq(this) :+ newValidation

    ValidationContainer(combinedSequence)
  }
}

/**
  * Class that contains a collection of Validation classes
  *
  * @param validations : set of validation contained in container class
  * @tparam T : Type for the validator classes
  */
case class ValidationContainer[T](validations: Seq[Validate[T]]) {
  /**
    * Add more validation to the list
    * and generate new ValidationContainer
    *
    * @param newValidation : validation class to be added
    * @return
    */
  def and(newValidation: Validate[T]): ValidationContainer[T] = {
    val combinedSequence: Seq[Validate[T]] = validations :+ newValidation

    ValidationContainer(combinedSequence)
  }
}

object ValidationContainer {
  /**
    * Implicit apply that gives us ability to use single validation class as ValidationContainer
    * so that we dont need to wrap that class in the container classes
    *
    * @param validation : validation class to be wrapped in container
    * @tparam T : Type for the validator classes
    * @return
    */
  implicit def apply[T](validation: Validate[T]): ValidationContainer[T] = ValidationContainer(Seq(validation))
}

/**
  * Trait to define property/behaviour of field validators
  *
  * @tparam T : Type for the validator classes
  */
trait FieldValidator[T] {
  //Field name to be validated
  val fieldName: String
  //validations to be performed on the field
  val validations: ValidationContainer[T]

  //perform the validation for given/all group
  def validate(group: Option[String] = None): Seq[String]
}