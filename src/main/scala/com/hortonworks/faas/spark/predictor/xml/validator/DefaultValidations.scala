package com.hortonworks.faas.spark.predictor.xml.validator

/**
  * Object containing set of default validations
  */
object DefaultValidations {

  /**
    * Check if the string start with the given value
    *
    * @param startString : value that the checked value should start with
    * @param groups      : Validation Groups current validation is related to
    */
  case class StartsWith(startString: String, groups: Seq[String] = Seq.empty) extends Validate[String] {
    /**
      * Main method that performs validation
      *
      * @param checkVal : value to check for validation
      * @return
      */
    def validate(checkVal: String): Boolean = checkVal.startsWith(startString)

    /**
      * Get the error if the validation has failed
      * This will always return error validation for each validator and is not tied to validation success/fail
      *
      * @param field : field used for validation
      * @return
      */
    def getError(field: String): String = s"$field does not start with $startString"
  }

  /**
    *
    * @param min    : minimun length of string
    * @param max    : maximum length of string
    * @param groups : Validation Groups current validation is related to
    */
  case class Length(min: Int = -1, max: Int = -1, groups: Seq[String] = Seq.empty) extends Validate[String] {
    //variable to hold the error
    private var error: String = ""

    /**
      * Main method that performs validation
      *
      * @param checkVal : value to check for validation
      * @return
      */
    def validate(checkVal: String): Boolean = {
      val valid = if (min != -1 && checkVal.length >= min) {
        error = if (max != -1) s"min length of $min and max length of $max" else s"min length of $min"
        false
      }
      else if (max != -1 && checkVal.length <= max) {
        error = if (min != -1) s"min length of $min and max length of $max" else s"max length of $min"
        false
      }
      else true

      valid
    }

    /**
      * Get the error if the validation has failed
      * This will always return error validation for each validator and is not tied to validation success/fail
      *
      * @param field : field used for validation
      * @return
      */
    def getError(field: String): String = s"$field must have $error"
  }

  /**
    *
    * @param min    : minimum value
    * @param max    : max value
    * @param groups : Validation Groups current validation is related to
    */
  case class InBetween(min: Int = -11111, max: Int = -11111, groups: Seq[String] = Seq.empty) extends Validate[Int] {
    //variable to hold the error
    private var error: String = ""

    /**
      * Main method that performs validation
      *
      * @param checkVal : value to check for validation
      * @return
      */
    def validate(checkVal: Int): Boolean = {
      val valid = if (min != -11111 && checkVal >= min) {
        error = if (max != -11111) s"min value of $min and max value of $max" else s"min value of $min"
        false
      }
      else if (max != -11111 && checkVal <= max) {
        error = if (min != -11111) s"min value of $min and max value of $max" else s"max value of $min"
        false
      }
      else true

      valid
    }

    /**
      * Get the error if the validation has failed
      * This will always return error validation for each validator and is not tied to validation success/fail
      *
      * @param field : field used for validation
      * @return
      */
    def getError(field: String): String = s"$field must have $error"
  }

}