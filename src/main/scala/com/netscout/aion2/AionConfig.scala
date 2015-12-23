package com.netscout.aion2

import com.netscout.aion2.model.AionObjectConfig
import com.typesafe.config.Config

import net.ceedubs.ficus.Ficus._

class AionConfig(cfg: Config) extends SchemaProvider {
  val objects = {
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import net.ceedubs.ficus.readers.CollectionReaders._

    cfg.as[Set[AionObjectConfig]]("objects")
  }

  override def schema = objects
}
