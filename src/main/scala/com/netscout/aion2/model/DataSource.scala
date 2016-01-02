package com.netscout.aion2.model

trait DataSource {
  def executeQuery(obj: AionObjectConfig, index: AionIndexConfig, query: QueryStrategy, partitionConstraints: Map[String, AnyRef]): Iterable[Iterable[(String, Object)]]
}
