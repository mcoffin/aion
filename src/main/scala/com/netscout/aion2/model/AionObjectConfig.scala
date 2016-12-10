package com.netscout.aion2.model

import com.typesafe.config.Config

import scala.beans.BeanProperty

class AionObjectConfig {
  @BeanProperty var name: String = null
  @BeanProperty var fields: java.util.Map[String, String] = null
  @BeanProperty var indices: java.util.List[AionIndexConfig] = null
}

class AionIndexConfig {
  @BeanProperty var name: String = null
  @BeanProperty var partition: java.util.List[String] = null
  @BeanProperty var split: AionSplitKeyConfig = null
  @BeanProperty var clustering: AionClusteringConfig = null
}

class AionSplitKeyConfig {
  @BeanProperty var column: String = null
  @BeanProperty var strategy: AionSplitStrategyConfig = null
}

class AionSplitStrategyConfig {
  import com.netscout.aion2.split.SplitStrategies
  import scala.collection.JavaConverters._

  @BeanProperty var name: String = null
  @BeanProperty var config: java.util.Map[String, String] = null

  def strategy = SplitStrategies.createStrategy(name, Option(config.asScala.toMap))
}

class AionClusteringConfig {
  @BeanProperty var field: String = null
  @BeanProperty var order: String = null
}
