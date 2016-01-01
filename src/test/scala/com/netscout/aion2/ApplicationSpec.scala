package com.netscout.aion2

import org.scalatest._

class ApplicationSpec extends FlatSpec with Matchers {
  import com.typesafe.config.ConfigFactory

  val defaultConfig = ConfigFactory.parseResources(classOf[ApplicationSpec], "defaults.json")

  it should "correctly prepend the configuration path" in {
    val app = new Application(defaultConfig)
    app.getConfigKey("dataSource") should be ("com.netscout.aion2.dataSource")
  }

  it should "be initializable with minimal configuration" in {
    val uut = new Application(defaultConfig)
    uut should not be (null)
  }
}
