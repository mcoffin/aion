package com.netscout.aion2.split

import com.netscout.aion2.SplitStrategy
import com.typesafe.config.Config

object SplitStrategies {
  def createStrategy(name: String, cfg: Option[Map[String, String]]): SplitStrategy = {
    name match {
      case "duration" => new DurationSplitStrategy(cfg)
      case _ => throw new Exception(s"Invalid strategy name: ${name}")
    }
  }
}
