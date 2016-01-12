package com.netscout.aion2.inject

import com.google.inject.AbstractModule
import com.google.inject.name.Names

import net.codingwell.scalaguice.ScalaModule

/**
 * Guice module for binding named system properties
 */
object SystemPropertiesModule extends AbstractModule with ScalaModule {
  override def configure {
    val props = System.getProperties()
    Names.bindProperties(binder(), props)
  }
}
