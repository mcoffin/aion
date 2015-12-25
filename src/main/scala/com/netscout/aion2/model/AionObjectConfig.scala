package com.netscout.aion2.model

import com.typesafe.config.Config

case class AionObjectConfig (
  fields: Map[String, String],
  indices: Set[AionIndexConfig]
)

case class AionIndexConfig (
  name: String,
  partition: Seq[String],
  split: AionSplitKeyConfig,
  range: Seq[String],
  clustering: AionClusteringConfig
)

case class AionSplitKeyConfig (
  column: String,
  strategy: AionSplitStrategyConfig
)

case class AionSplitStrategyConfig (
  name: String,
  config: Option[Config]
) {
  import com.netscout.aion2.split.SplitStrategies

  def strategy = SplitStrategies.createStrategy(name, config)
}

case class AionClusteringConfig (
  field: String,
  order: String
)
