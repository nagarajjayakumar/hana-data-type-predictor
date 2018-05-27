package com.hortonworks.faas.spark.predictor.inference_engine.analytic.common.analytic

object SamplingTechniqType extends Enumeration {
  /*
  Technically, “reservoir sampling” is defined as group of algorithms for
  selecting N records from a list whose length is unknown.
   */
  val STRT_RSVR_SMPL = Value("STARTIFIED_RESERVOIR_SAMPLING")
  /*
  Select X% of records from each group.
  So if one group has 100 records and another group has 10K records
  and we want to select 10% records from each group
  then the sample output should contain 10 records from the first group
  and 1K records from the second group.
   */
  val STRT_CONST_PROP = Value("STRATIFIED_CONSTANT_PROPORTION")

  val STRT_DYNMC_POPL = Value("HANA_STARTIFIED_DYNAMIC_POPULATION")
  /*
  In other words all rows are equally weighted
   */
  val RNDM_SMPL = Value("RANDOM_SAMPLING")
  val Unknown = Value("Unknown")


  def withNameWithDefault(name: String): Value =
    values.find(_.toString.toLowerCase == name.toLowerCase()).getOrElse(Unknown)

}