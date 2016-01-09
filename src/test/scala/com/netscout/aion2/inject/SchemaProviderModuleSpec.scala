package com.netscout.aion2.inject

import com.google.inject.{Guice, Inject}
import com.netscout.aion2.SchemaProvider

import org.scalatest._

class TestInjected @Inject() (
  val schemaProvider: SchemaProvider
)

class SchemaProviderModuleSpec extends FlatSpec with Matchers {
  "A SchemaProviderModule" should "inject a schema provider built from schema.yml" in {
    import net.codingwell.scalaguice.InjectorExtensions._

    val injector = Guice.createInjector(AionResourceModule, SchemaProviderModule)
    val testInjected = injector.instance[TestInjected]
    testInjected.schemaProvider should not be (null)
    testInjected.schemaProvider.schema.size shouldBe 0
  }
}
