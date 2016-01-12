package com.netscout.aion2.model

/**
 * Trait representing the abstract idea of an Aion data storage engine
 */
trait DataSource {
  /**
   * Gets the desired class to be read for a given type
   *
   * @param t the name of the type to be mapped to t class
   */
  def classOfType(t: String): Class[_]

  /**
   * Initializes the backend storage for a given keyspace and set of objects
   *
   * @param objects the object schemata that should be created within the keyspace
   */
  def initializeSchema(objects: Set[AionObjectConfig])

  /**
   * Inserts data in to this data source
   *
   * @param obj the AionObjectConfig from the schema for which we are inserting
   * @param values the values provided for the object's fields
   */
  def insertQuery(obj: AionObjectConfig, values: Map[String, AnyRef])

  /**
   * Executes a SELECT-style query on the data source
   *
   * @param obj the [[com.netscout.aion2.model.AionObjectConfig]] for which to get data
   * @param index the [[com.netscout.aion2.model.AionIndexConfig]] from which to read data
   * @param query the [[com.netscout.aion2.model.QueryStrategy]] for reading from the split key
   * @param partitionConstraints map representing partition key values for the given index
   * @return an iterator over objects (represented as an iterator of tuples which represent the object's fields and values)
   */
  def executeQuery(obj: AionObjectConfig, index: AionIndexConfig, query: QueryStrategy, partitionConstraints: Map[String, AnyRef]): Iterable[Iterable[(String, Object)]]
}
