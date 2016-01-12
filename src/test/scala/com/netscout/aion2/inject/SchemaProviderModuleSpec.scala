package com.netscout.aion2.inject

import com.google.inject.{Guice, Inject}
import com.netscout.aion2.SchemaProvider

import org.scalatest._

class TestInjected @Inject() (
  val schemaProvider: SchemaProvider
)

class SchemaProviderModuleSpec extends FlatSpec with Matchers {
  import net.codingwell.scalaguice.InjectorExtensions._

  "A SchemaProviderModule" should "inject a schema provider built from schema.yml" in {
    val injector = Guice.createInjector(SystemPropertiesModule, AionResourceModule, SchemaProviderModule)
    val testInjected = injector.instance[TestInjected]
    testInjected.schemaProvider should not be (null)
    testInjected.schemaProvider.schema.size shouldBe 0
  }

  it should "inject a schema provider built from specified file" in {
    System.setProperty("com.netscout.aion2.schemaFile", "src/test/resources/com/netscout/aion2/schema-overridden.yml")
    val injector = Guice.createInjector(SystemPropertiesModule, AionResourceModule, SchemaProviderModule)
    val testInjected = injector.instance[TestInjected]
    testInjected.schemaProvider should not be (null)
    testInjected.schemaProvider.schema.size shouldBe 1
  }
}
