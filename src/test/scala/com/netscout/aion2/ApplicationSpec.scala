package com.netscout.aion2

import com.github.racc.tscg.TypesafeConfigModule
import com.google.inject.{AbstractModule, Guice}
import com.netscout.aion2.inject._
import com.netscout.aion2.model.DataSource

import javax.ws.rs.core.{Application => JAXRSApplication, Response}

import net.codingwell.scalaguice.ScalaModule

import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.JerseyTest
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.BDDMockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

class ApplicationSpec extends FlatSpec with Matchers with MockitoSugar {
  import com.typesafe.config.ConfigFactory
  import scala.collection.JavaConversions._

  class ApplicationJerseyTest (
    val application: JAXRSApplication
  ) extends JerseyTest(application)

  class TestModule (
    val name: String
  ) extends AbstractModule with ScalaModule {
    val resourceConfig = new ResourceConfig
    val dataSource = mock[DataSource]

    override def configure {
      bind[ResourceConfig].toInstance(resourceConfig)
      bind[SchemaProvider].toInstance(new AionConfig(classOf[ApplicationSpec].getResourceAsStream(s"schema-${name}.yml")))
      bind[DataSource].toInstance(dataSource)
    }
  }

  def namedConfig(name: String) = ConfigFactory.parseResources(classOf[ApplicationSpec], name ++ ".json")

  def namedApplication(name: String, testModule: Option[TestModule] = None) = {
    import net.codingwell.scalaguice.InjectorExtensions._

    val tModule = testModule match {
      case Some(m) => m
      case None => new TestModule(name)
    }

    val injector = Guice.createInjector(
      TypesafeConfigModule.fromConfig(namedConfig(name)),
      JacksonModule,
      tModule)

    injector.instance[Application]
  }

  def namedFixture(name: String) =
    new {
      val testModule = new TestModule(name)
      val app = namedApplication(name, Some(testModule))
      val test = new ApplicationJerseyTest(testModule.resourceConfig)
      test.setUp()
    }

  def defaultFixture = namedFixture("defaults")
  def defaultApplication = namedApplication("defaults")

  implicit class TestApplicationHelper(val app: JAXRSApplication) {
    def resourceCount = app.getClasses.size + app.getSingletons.size
  }

  it should "be initializable with minimal configuration" in {
    val uut = defaultApplication
    uut should not be (null)
  }

  it should "not register any resources with no objects" in {
    val f = defaultFixture
    val uut = f.app
    f.testModule.resourceConfig.getClasses should not be (null)
    f.testModule.resourceConfig.getSingletons should not be (null)
    f.testModule.resourceConfig.resourceCount should be (0)
  }

  it should "register resources of complete schema" in {
    val f = namedFixture("complete")
    val uut = f.app
    val resourceConfig = f.testModule.resourceConfig
    val registeredResources = resourceConfig.getResources
    val expectedIndexCount = 3
    registeredResources.size should be (2 * expectedIndexCount)
    val resourcePaths = registeredResources.map(r => (r.getPath, r)).toMap

    val fullIndexPath = "/foo"
    val fullResourcePath = "/foo/{partition}{rangeKeys: ((/([\\w\\.\\d\\-%]+)){1,1})?}"
    resourcePaths.keys.contains(fullIndexPath) shouldBe true
    resourcePaths.keys.contains(fullResourcePath) shouldBe true

    val noRangeIndexPath = "/bar"
    val noRangeResourcePath = "/bar/{partition}"
    resourcePaths.keys.contains(noRangeIndexPath) shouldBe true
    resourcePaths.keys.contains(noRangeResourcePath) shouldBe true

    val noPartitionPath = "/no_partition"
    resourcePaths.keys.contains(noPartitionPath) shouldBe true
  }

  it should "register two resources of identical path for indices without partition keys" in {
    val f = namedFixture("complete")
    val uut = f.app
    val resourceConfig = f.testModule.resourceConfig
    val registeredResources = resourceConfig.getResources
    val noPartitionResources = registeredResources.filter(r => r.getPath.equals("/no_partition"))
    noPartitionResources.size shouldBe 2
  }

  "The resource resource" should "ask the DataSource for data on HTTP GET" in {
    import java.time.Instant
    import java.time.temporal.ChronoUnit._

    val f = namedFixture("complete")

    // given
    given(f.testModule.dataSource.executeQuery(anyObject(), anyObject(), anyObject(), anyObject())).willReturn(Seq())

    // when
    val now = Instant.now
    val end = now.plus(1, HOURS)
    val result: Response = f.test.target(s"/bar/somePartition").queryParam("from", now.toString).queryParam("to", end.toString).request().get()

    // then
    result.getStatus shouldBe 200
    verify(f.testModule.dataSource).executeQuery(anyObject(), anyObject(), anyObject(), anyObject())

    f.test.tearDown
  }
}
