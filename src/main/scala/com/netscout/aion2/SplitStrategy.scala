package com.netscout.aion2

import com.netscout.aion2.model.QueryStrategy

import javax.ws.rs.core.MultivaluedMap

trait SplitStrategy {
  def strategyForQuery(parameters: MultivaluedMap[String, String]): QueryStrategy
}
