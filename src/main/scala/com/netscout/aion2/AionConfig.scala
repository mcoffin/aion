package com.netscout.aion2

import com.google.inject.Inject
import com.netscout.aion2.model.AionObjectConfig
import com.typesafe.config.Config

import java.io.InputStream

import org.yaml.snakeyaml.Yaml

import scala.beans.BeanProperty

/**
 * Convenience class that models the structure of Aion's
 * schema.yml file
 */
class Configuration {
  /**
   * Set of objects that describe Aion's keyspace
   */
  @BeanProperty var objects: Array[AionObjectConfig] = null
}

/**
 * Convenience class for using Aion's schema.yml configuration
 * as an instance of [[com.netscout.aion2.SchemaProvider]]
 */
class AionConfig (
  val inputStream: InputStream
) extends SchemaProvider {
  import scala.collection.JavaConversions._

  /**
   * Lazy-loaded instance of [[com.netscout.aion2.Configuration]] built
   * from the povided InputStream
   */
  lazy val cfg: Configuration = {
    val yaml = new Yaml
    yaml.loadAs(inputStream, classOf[Configuration])
  }

  override def schema = Set(cfg.objects : _*)
}
