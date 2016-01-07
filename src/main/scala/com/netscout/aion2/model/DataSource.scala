package com.netscout.aion2.model

trait DataSource {
  def classOfType(t: String): Class[_]
  def initializeSchema(objects: Set[AionObjectConfig])
  def insertQuery(obj: AionObjectConfig, values: Map[String, AnyRef])
  def executeQuery(obj: AionObjectConfig, index: AionIndexConfig, query: QueryStrategy, partitionConstraints: Map[String, AnyRef]): Iterable[Iterable[(String, Object)]]
}
