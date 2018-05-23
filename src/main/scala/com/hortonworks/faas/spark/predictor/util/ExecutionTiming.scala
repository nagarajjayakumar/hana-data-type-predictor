package com.hortonworks.faas.spark.predictor.util


trait ExecutionTiming extends Logging {
  // Spark uses the log4j logger by default

  def time[R](name: String, block: => R): R = {
    val start = System.currentTimeMillis()
    val result = block
    // call-by-name
    val end = System.currentTimeMillis()
    logInfo(s"Execution of '${name}' - Elapsed time: ${end - start} ms")
    result
  }
}
