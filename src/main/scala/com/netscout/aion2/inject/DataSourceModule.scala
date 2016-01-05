package com.netscout.aion2.inject

import com.google.inject.{Guice, AbstractModule}
import com.netscout.aion2.model.DataSource
import com.netscout.aion2.source.CassandraDataSource

import net.codingwell.scalaguice.ScalaModule

object DataSourceModule extends AbstractModule with ScalaModule {
  override def configure {
    bind[DataSource].to[CassandraDataSource]
  }
}
