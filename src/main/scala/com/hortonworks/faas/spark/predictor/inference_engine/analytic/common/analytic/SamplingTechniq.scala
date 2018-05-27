package com.hortonworks.faas.spark.predictor.inference_engine.analytic.common.analytic

object SamplingTechniq extends Enumeration {
  val STRT_RSVR_SMPL = Value("STARTIFIED_RESERVOIR_SAMPLING")
  val STRT_CONST_PROP = Value("STRATIFIED_CONSTANT_PROPORTION")
  val RNDM_SMPL = Value("RANDOM_SAMPLING")
  val Unknown = Value("Unknown")


  def withNameWithDefault(name: String): Value =
    values.find(_.toString.toLowerCase == name.toLowerCase()).getOrElse(Unknown)

}