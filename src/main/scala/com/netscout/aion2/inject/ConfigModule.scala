package com.netscout.aion2.inject

import com.google.inject.{Guice, AbstractModule}
import com.netscout.aion2.{AionConfig, SchemaProvider}
import com.netscout.aion2.model.DataSource
import com.netscout.aion2.source.CassandraDataSource

import net.codingwell.scalaguice.ScalaModule

/**
 * Guice module for injecting built-ins configured by a typesafe
 * config file.
 */
object ConfigModule extends AbstractModule with ScalaModule {
  override def configure {
    bind[SchemaProvider].to[AionConfig]
    bind[DataSource].to[CassandraDataSource]
  }
}
