package com.netscout.aion2

import com.google.inject.Inject
import com.netscout.aion2.model.AionObjectConfig
import com.typesafe.config.Config

import java.io.InputStream

import org.yaml.snakeyaml.Yaml

import scala.beans.BeanProperty

class Configuration {
  @BeanProperty var objects: Array[AionObjectConfig] = null
}

class AionConfig (
  val inputStream: InputStream
) extends SchemaProvider {
  import scala.collection.JavaConversions._

  lazy val cfg: Configuration = {
    val yaml = new Yaml
    yaml.loadAs(inputStream, classOf[Configuration])
  }

  override def schema = Set(cfg.objects : _*)
}
