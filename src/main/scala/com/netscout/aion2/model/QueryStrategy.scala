package com.netscout.aion2.model

trait QueryStrategy {
  /**
   * Real minimum for the split key
   */
  def minimum: Object

  /**
   * Real maximum for the split key
   */
  def maximum: Object

  /**
   * Rows from which to query partial data
   */
  def partialRows: Iterable[Object]

  /**
   * Rows from which to query ALL data
   */
  def fullRows: Option[(Object, Object)]
}
