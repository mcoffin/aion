package com.netscout.aion2.inject

import com.google.inject.{Guice, AbstractModule, Inject}
import com.netscout.aion2.{AionConfig, SchemaProvider}

import java.io.InputStream

import net.codingwell.scalaguice.ScalaModule

class InjectedAionConfig @Inject() (
  @AionResource(resourcePath = "schema.yml") resourceStream: Option[InputStream]
) extends AionConfig(resourceStream.get)

/**
 * Guice module for injecting built-ins configured by a typesafe
 * config file.
 */
object SchemaProviderModule extends AbstractModule with ScalaModule {
  override def configure {
    bind[SchemaProvider].to[InjectedAionConfig]
  }
}
