package com.netscout.aion2

import com.netscout.aion2.model.QueryStrategy

import javax.ws.rs.core.MultivaluedMap

trait SplitStrategy {
  /**
   * Gets the row key value for a split key of this strategy
   *
   * @param value the actual value of the split key
   */
  def rowKey(value: Object): Object

  /**
   * Creates a query strategy for a given set of parameters
   */
  def strategyForQuery(parameters: MultivaluedMap[String, String]): QueryStrategy
}
