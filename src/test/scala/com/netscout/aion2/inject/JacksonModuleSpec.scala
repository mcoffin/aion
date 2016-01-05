package com.netscout.aion2.inject

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.{Guice, Inject}

import org.scalatest._

class TestInjectedJackson @Inject() (
  val mapper: ObjectMapper
)

class JacksonModuleSpec extends FlatSpec with Matchers {
  "A JacksonModule" should "inject an ObjectMapper" in {
    import net.codingwell.scalaguice.InjectorExtensions._

    val injector = Guice.createInjector(JacksonModule)
    val testInjected = injector.instance[TestInjectedJackson]
    testInjected.mapper should not be (null)
  }
}
