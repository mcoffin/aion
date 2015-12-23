package com.netscout.aion2

import com.netscout.aion2.model.AionObjectConfig

trait SchemaProvider {
  def schema: Set[AionObjectConfig]
}
