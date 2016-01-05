package com.netscout.aion2

import com.github.racc.tscg.TypesafeConfigModule
import com.google.inject.{AbstractModule, Guice}
import com.netscout.aion2.inject._
import com.netscout.aion2.model.DataSource

import javax.ws.rs.core.{Application => JAXRSApplication}

import net.codingwell.scalaguice.ScalaModule

import org.glassfish.jersey.server.ResourceConfig
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

class ApplicationSpec extends FlatSpec with Matchers with MockitoSugar {
  import com.typesafe.config.ConfigFactory
  import scala.collection.JavaConversions._

  val resourceConfig = new ResourceConfig
  val dataSource = mock[DataSource]

  class TestModule (
    val name: String
  ) extends AbstractModule with ScalaModule {
    override def configure {
      bind[ResourceConfig].toInstance(resourceConfig)
      bind[SchemaProvider].toInstance(new AionConfig(classOf[ApplicationSpec].getResourceAsStream(s"schema-${name}.yml")))
      bind[DataSource].toInstance(dataSource)
    }
  }

  def namedConfig(name: String) = ConfigFactory.parseResources(classOf[ApplicationSpec], name ++ ".json")

  def namedApplication(name: String) = {
    import net.codingwell.scalaguice.InjectorExtensions._

    val injector = Guice.createInjector(
      TypesafeConfigModule.fromConfig(namedConfig(name)),
      JacksonModule,
      new TestModule(name))

    injector.instance[Application]
  }

  def defaultApplication = namedApplication("defaults")

  implicit class TestApplicationHelper(val app: JAXRSApplication) {
    def resourceCount = app.getClasses.size + app.getSingletons.size
  }

  it should "be initializable with minimal configuration" in {
    val uut = defaultApplication
    uut should not be (null)
  }

  it should "not register any resources with no objects" in {
    val uut = defaultApplication
    resourceConfig.getClasses should not be (null)
    resourceConfig.getSingletons should not be (null)
    resourceConfig.resourceCount should be (0)
  }

  it should "register 2 resources with complete configuration" in {
    val uut = namedApplication("complete")
    val registeredResources = resourceConfig.getResources
    val indexCount = 2
    registeredResources.size should be (2 * indexCount)
    val resourcePaths = registeredResources.map(r => (r.getPath, r)).toMap
    val fullIndexPath = "/foo"
    val fullResourcePath = "/foo/{partition}{rangeKeys: ((/([\\w\\.\\d\\-%]+)){1,1})?}"
    resourcePaths.keys.contains(fullIndexPath) shouldBe true
    resourcePaths.keys.contains(fullResourcePath) shouldBe true
    val noRangeIndexPath = "/bar"
    val noRangeResourcePath = "/bar/{partition}"
    resourcePaths.keys.contains(noRangeIndexPath) shouldBe true
    resourcePaths.keys.contains(noRangeResourcePath) shouldBe true
  }
}
