package com.netscout.aion2.inject

import com.google.inject.{Guice, AbstractModule}
import com.netscout.aion2.{AionConfig, SchemaProvider}

import net.codingwell.scalaguice.ScalaModule

/**
 * Guice module for injecting built-ins configured by a typesafe
 * config file.
 */
object SchemaProviderModule extends AbstractModule with ScalaModule {
  override def configure {
    bind[SchemaProvider].toInstance(new AionConfig(classOf[AionConfig].getResourceAsStream("schema.yml")))
  }
}
