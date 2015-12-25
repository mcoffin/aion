package com.netscout.aion2.model

trait DataSource {
  def executeQuery(obj: AionObjectConfig, index: AionIndexConfig, query: QueryStrategy): Map[String, Object]
}
