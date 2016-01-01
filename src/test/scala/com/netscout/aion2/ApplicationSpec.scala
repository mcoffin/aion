package com.netscout.aion2

import javax.ws.rs.core.{Application => JAXRSApplication}

import org.scalatest._

class ApplicationSpec extends FlatSpec with Matchers {
  import com.typesafe.config.ConfigFactory

  def namedConfig(name: String) = ConfigFactory.parseResources(classOf[ApplicationSpec], name ++ ".json")
  def namedApplication(name: String) = new Application(namedConfig(name))

  val defaultConfig = namedConfig("defaults")
  def defaultApplication = new Application(defaultConfig)

  implicit class TestApplicationHelper(val app: JAXRSApplication) {
    def resourceCount = app.getClasses.size + app.getSingletons.size
  }

  it should "correctly prepend the configuration path" in {
    val uut = defaultApplication
    uut.getConfigKey("dataSource") should be ("com.netscout.aion2.dataSource")
  }

  it should "be initializable with minimal configuration" in {
    val uut = defaultApplication
    uut should not be (null)
  }

  it should "not register any resources with no objects" in {
    val uut = defaultApplication
    uut.getClasses should not be (null)
    uut.getSingletons should not be (null)
    uut.resourceCount should be (0)
  }
}
