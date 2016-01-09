package com.netscout.aion2.source

import com.google.inject.{Guice, Module, AbstractModule}

import net.codingwell.scalaguice.ScalaModule
import net.codingwell.scalaguice.InjectorExtensions._

import org.scalatest._
import org.scalatest.mock.MockitoSugar

class CassandraDataSourceSpec extends FlatSpec with Matchers with MockitoSugar {
  import com.netscout.aion2.ApplicationSpec
  import com.typesafe.config.ConfigFactory

  class MockedCassandraModule extends AbstractModule with ScalaModule {
    import com.datastax.driver.core.Session

    val session = mock[Session]

    override def configure {
      bind[Session].toInstance(session)
    }
  }

  def defaultModules = {
    import com.github.racc.tscg.TypesafeConfigModule
    import com.netscout.aion2.inject._

    Seq(
      TypesafeConfigModule.fromConfig(ApplicationSpec.namedConfig("defaults")),
      JacksonModule,
      Slf4jLoggerModule,
      AionResourceModule,
      new MockedCassandraModule
    )
  }

  "A CassandraDataSource" should "return JsonNode from classOfType for \"json\" type" in {
    import com.fasterxml.jackson.databind.JsonNode

    val injector = Guice.createInjector(defaultModules : _*)
    val uut = injector.instance[CassandraDataSource]

    uut.classOfType("json") shouldBe classOf[JsonNode]
  }
}
