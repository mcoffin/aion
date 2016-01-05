package com.netscout.aion2.model

import com.typesafe.config.Config

import scala.beans.BeanProperty

class AionObjectConfig {
  @BeanProperty var fields: java.util.Map[String, Object] = null
  @BeanProperty var indices: java.util.List[AionIndexConfig] = null
}

//case class AionObjectConfig (
//  fields: Map[String, String],
//  indices: Set[AionIndexConfig]
//)

class AionIndexConfig {
  @BeanProperty var name: String = null
  @BeanProperty var partition: java.util.List[String] = null
  @BeanProperty var split: AionSplitKeyConfig = null
  @BeanProperty var range: java.util.List[String] = null
  @BeanProperty var clustering: AionClusteringConfig = null
}

//case class AionIndexConfig (
//  name: String,
//  partition: Seq[String],
//  split: AionSplitKeyConfig,
//  range: Seq[String],
//  clustering: AionClusteringConfig
//)

class AionSplitKeyConfig {
  @BeanProperty var column: String = null
  @BeanProperty var strategy: AionSplitStrategyConfig = null
}

//case class AionSplitKeyConfig (
//  column: String,
//  strategy: AionSplitStrategyConfig
//)

class AionSplitStrategyConfig {
  import com.netscout.aion2.split.SplitStrategies
  import scala.collection.JavaConversions._

  @BeanProperty var name: String = null
  @BeanProperty var config: java.util.Map[String, String] = null

  def strategy = SplitStrategies.createStrategy(name, Option(config.toMap))
}

//case class AionSplitStrategyConfig (
//  name: String,
//  config: Option[Config]
//) {
//  import com.netscout.aion2.split.SplitStrategies
//
//  def strategy = SplitStrategies.createStrategy(name, config)
//}

class AionClusteringConfig {
  @BeanProperty var field: String = null
  @BeanProperty var order: String = null
}

//case class AionClusteringConfig (
//  field: String,
//  order: String
//)
