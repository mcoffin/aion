package com.netscout.aion2.model

trait DataSource {
  def insertQuery(obj: AionObjectConfig, index: AionIndexConfig, values: Map[String, AnyRef])
  def executeQuery(obj: AionObjectConfig, index: AionIndexConfig, query: QueryStrategy, partitionConstraints: Map[String, AnyRef]): Iterable[Iterable[(String, Object)]]
}
