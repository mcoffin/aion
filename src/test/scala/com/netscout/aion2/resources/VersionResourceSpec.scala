package com.netscout.aion2.resources

import com.google.inject.Guice
import com.netscout.aion2.ResourceConfigUtils
import com.netscout.aion2.inject.Slf4jLoggerModule

import javax.ws.rs.core.Response
import javax.ws.rs.core.MediaType._

import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.JerseyTest
import org.scalatest._

class VersionResourceSpec extends FlatSpec with Matchers {
  def versionResource = {
    import net.codingwell.scalaguice.InjectorExtensions._

    val injector = Guice.createInjector(Slf4jLoggerModule)
    injector.instance[VersionResource]
  }

  def resourceConfig = {
    val cfg = new ResourceConfig
    ResourceConfigUtils.register(cfg, versionResource)
  }

  class VersionResourceJerseyTest extends JerseyTest(resourceConfig) {
    setUp()
  }

  val jerseyTest = new VersionResourceJerseyTest
  def fixture =
    new {
      val test = jerseyTest
    }

  "A version resource" should "return the project version" in {
    import com.netscout.aion2.Application
    import java.util.Properties

    val f = fixture
    val propertiesShould = new Properties
    propertiesShould.load(classOf[Application].getResourceAsStream("version.properties"))

    val response: Response = f.test.target("/version").request.get

    response.getStatusInfo.getFamily shouldBe Response.Status.Family.SUCCESSFUL
    val versionString = response.readEntity(classOf[String])
    versionString shouldEqual propertiesShould.getProperty("version")
  }

  it should "return text/plain content type" in {
    val f = fixture

    val response: Response = f.test.target("/version").request.get

    response.getStatusInfo.getFamily shouldBe Response.Status.Family.SUCCESSFUL
    response.getMediaType shouldBe TEXT_PLAIN_TYPE
  }
}
