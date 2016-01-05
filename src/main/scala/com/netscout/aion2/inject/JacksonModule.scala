package com.netscout.aion2.inject

import com.google.inject.AbstractModule

import net.codingwell.scalaguice.ScalaModule

object JacksonModule extends AbstractModule with ScalaModule {
  import com.fasterxml.jackson.databind.ObjectMapper
  import com.fasterxml.jackson.module.scala.DefaultScalaModule

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def configure {
    bind[ObjectMapper].toInstance(mapper)
  }
}
