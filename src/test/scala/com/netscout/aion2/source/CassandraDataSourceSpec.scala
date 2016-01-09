package com.netscout.aion2.source

import com.google.inject.{Guice, Module, AbstractModule}

import net.codingwell.scalaguice.ScalaModule
import net.codingwell.scalaguice.InjectorExtensions._

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

class CassandraDataSourceSpec extends FlatSpec with Matchers with MockitoSugar {
  import com.netscout.aion2.{AionConfig, ApplicationSpec}
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
      AionResourceModule
    )
  }

  def defaultFixture =
    new {
      val testModule = new MockedCassandraModule
      val injector = Guice.createInjector((defaultModules :+ testModule): _*)
      val uut = injector.instance[CassandraDataSource]
    }

  "A CassandraDataSource" should "return JsonNode from classOfType for \"json\" type" in {
    import com.fasterxml.jackson.databind.JsonNode

    val f = defaultFixture

    f.uut.classOfType("json") shouldBe classOf[JsonNode]
  }

  it should "create 1 tables per index for schema in initializeSchema()" in {
    val schemaProvider = new AionConfig(classOf[ApplicationSpec].getResourceAsStream("schema-complete.yml"))

    val f = defaultFixture
    f.uut.initializeSchema(schemaProvider.schema)

    // Because there's 3 indices 
    verify(f.testModule.session, times(3)).execute(anyString())
  }
}
