package com.hortonworks.faas.spark.predictor.orm.setting

import scalikejdbc._
import scalikejdbc.config._

trait DBSettings {
  def env: String

  DBSettings.initialize(env)
}

object DBSettings {

  private var isInitialized = false

  def initialize(env: String): Unit = this.synchronized {
    if (isInitialized) return
    DBsWithEnv(env).setupAll()
    GlobalSettings.loggingSQLErrors = false
    GlobalSettings.sqlFormatter = SQLFormatterSettings("utils.HibernateSQLFormatter")
    isInitialized = true
  }

}
