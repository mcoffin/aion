package com.netscout.aion2.source

import com.netscout.aion2.model.{AionObjectConfig, AionIndexConfig, QueryStrategy, DataSource}
import com.typesafe.config.{Config, ConfigException}

class CassandraDataSource(cfg: Option[Config]) extends DataSource {
  val config = cfg match {
    case Some(x) => x
    case None => throw new ConfigException.Missing(s"dataSource")
  }

  override def executeQuery(obj: AionObjectConfig, index: AionIndexConfig, query: QueryStrategy) = {
    Map()
  }
}
