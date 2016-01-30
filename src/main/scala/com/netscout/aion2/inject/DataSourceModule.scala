package com.netscout.aion2.inject

import com.datastax.driver.core.{Cluster, Session}
import com.github.racc.tscg.TypesafeConfig
import com.google.inject.{Guice, AbstractModule, Provider, Inject}
import com.netscout.aion2.model.DataSource
import com.netscout.aion2.source.CassandraDataSource

import net.codingwell.scalaguice.ScalaModule

class CassandraSessionProvider @Inject() (
  @TypesafeConfig("com.netscout.aion2.cassandra.contactPoints") contactPoints: java.util.List[String],
  @TypesafeConfig("com.netscout.aion2.cassandra.port") cassandraPort: Integer
) extends Provider[Session] {
  import scala.collection.JavaConversions._

  val cluster = Cluster.builder()
    .addContactPoints(contactPoints : _*)
    .withPort(cassandraPort)
    .build()

  lazy val session = cluster.connect()

  override def get = session
}

object DataSourceModule extends AbstractModule with ScalaModule {
  override def configure {
    bind[Session].toProvider[CassandraSessionProvider]
    bind[DataSource].to[CassandraDataSource]
  }
}
