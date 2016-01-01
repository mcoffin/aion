package com.netscout.aion2.model

/**
 * Convenience query strategy representing an empty range
 */
object EmptyQueryStrategy extends QueryStrategy {
  override def minimum = null
  override def maximum = null
  override def partialRows = Seq()
  override def fullRows = None
}
