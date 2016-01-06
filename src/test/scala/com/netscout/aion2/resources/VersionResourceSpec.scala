package com.netscout.aion2.resources

import com.google.inject.{AbstractModule, Guice, Provider}
import com.netscout.aion2.ResourceConfigUtils
import com.netscout.aion2.inject._

import java.io.InputStream

import javax.ws.rs.core.Response
import javax.ws.rs.core.MediaType._

import net.codingwell.scalaguice.ScalaModule
import net.codingwell.scalaguice.InjectorExtensions._

import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.JerseyTest
import org.scalatest._

class VersionResourceSpec extends FlatSpec with Matchers {
  def versionResource = {
    val injector = Guice.createInjector(Slf4jLoggerModule, AionResourceModule)
    injector.instance[VersionResource]
  }

  def resourceConfig = {
    val cfg = new ResourceConfig
    ResourceConfigUtils.register(cfg, versionResource)
    cfg
  }

  class VersionResourceJerseyTest (
    val app: ResourceConfig
  ) extends JerseyTest(app) {
    setUp()
  }

  val jerseyTest = new VersionResourceJerseyTest(resourceConfig)
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

  class NoVersionPropertiesModule extends AbstractModule with ScalaModule {
    import com.google.inject.util.Providers

    override def configure {
      bind[Option[InputStream]].annotatedWith(new VersionAionResource).toInstance(None)
    }
  }

  it should "return 404 if version.properties doesn't exist" in {
    val injector = Guice.createInjector(
      Slf4jLoggerModule,
      new NoVersionPropertiesModule
    )

    val cfg = new ResourceConfig
    ResourceConfigUtils.register(cfg, injector.instance[VersionResource])

    val test = new VersionResourceJerseyTest(cfg)
    val response: Response = test.target("/version").request.get

    response.getStatus shouldBe 404
    
    test.tearDown
  }

  class BadVersionPropertiesModule extends AbstractModule with ScalaModule {
    import java.io.ByteArrayInputStream

    override def configure {
      bind[Option[InputStream]].annotatedWith(new VersionAionResource).toInstance(Some(new ByteArrayInputStream("".getBytes("UTF-8"))))
    }
  }

  it should "return 404 if version.properties doesn't have a version" in {
    val injector = Guice.createInjector(
      Slf4jLoggerModule,
      new BadVersionPropertiesModule
    )

    val cfg = new ResourceConfig
    ResourceConfigUtils.register(cfg, injector.instance[VersionResource])

    val test = new VersionResourceJerseyTest(cfg)
    val response: Response = test.target("/version").request.get

    response.getStatus shouldBe 404

    test.tearDown
  }
}
