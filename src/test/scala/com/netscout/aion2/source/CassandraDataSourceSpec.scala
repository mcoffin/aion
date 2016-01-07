package com.netscout.aion2.source

import com.google.inject.{Guice, Module}

import net.codingwell.scalaguice.InjectorExtensions._

import org.scalatest._

class CassandraDataSourceSpec extends FlatSpec with Matchers {
  import com.netscout.aion2.ApplicationSpec
  import com.typesafe.config.ConfigFactory

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

  "A CassandraDataSource" should "return JsonNode from classOfType for \"json\" type" in {
    import com.fasterxml.jackson.databind.JsonNode

    val injector = Guice.createInjector(defaultModules : _*)
    val uut = injector.instance[CassandraDataSource]

    uut.classOfType("json") shouldBe classOf[JsonNode]
  }
}
