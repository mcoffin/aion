package com.netscout.aion2

import com.github.racc.tscg.TypesafeConfig
import com.google.inject.Inject
import com.netscout.aion2.model.AionObjectConfig
import com.typesafe.config.Config

import org.yaml.snakeyaml.Yaml

import scala.beans.BeanProperty

class Configuration {
  @BeanProperty var objects: Array[AionObjectConfig] = null
}

class AionConfig extends SchemaProvider {
  import scala.collection.JavaConversions._

  lazy val cfg: Configuration = {
    val inputStream = Option(classOf[AionConfig].getClassLoader().getResourceAsStream("/schema.yml")) match {
      case Some(iStream) => iStream
      case None => throw new RuntimeException("Could not load schema file: schema.yml")
    }
    val yaml = new Yaml
    yaml.loadAs(inputStream, classOf[Configuration])
  }

  override def schema = Set(cfg.objects : _*)
}
