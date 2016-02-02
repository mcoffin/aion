package com.netscout.aion2

import com.github.racc.tscg.TypesafeConfigModule
import com.google.inject.{AbstractModule, Guice}
import com.netscout.aion2.inject._
import com.netscout.aion2.model.DataSource

import javax.ws.rs.core.{Application => JAXRSApplication, Response}

import net.codingwell.scalaguice.ScalaModule

import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.JerseyTest
import org.mockito.{Matchers => MockitoMatchers}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.BDDMockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

object ApplicationSpec {
  import com.typesafe.config.ConfigFactory

  /**
   * Gets a config file with a specific name
   */
  def namedConfig(name: String) = ConfigFactory.parseResources(this.getClass, name ++ ".json")
}

class ApplicationSpec extends FlatSpec with Matchers with MockitoSugar {
  import com.typesafe.config.ConfigFactory
  import scala.collection.JavaConversions._
  import javax.ws.rs.core.Response.Status.Family._
  import javax.ws.rs.core.MediaType._

  class ApplicationJerseyTest (
    val application: JAXRSApplication
  ) extends JerseyTest(application)

  class TestModule (
    val name: String
  ) extends AbstractModule with ScalaModule {
    val resourceConfig = new ResourceConfig
    val dataSource = mock[DataSource]

    def setupTestDataTypes {
      import java.util.UUID

      doReturn(classOf[String], Seq.empty : _*).when(dataSource).classOfType("text")
      doReturn(classOf[UUID], Seq.empty : _*).when(dataSource).classOfType("timeuuid")
      doReturn(classOf[Array[Byte]], Seq.empty : _*).when(dataSource).classOfType("blob")
    }

    override def configure {
      bind[ResourceConfig].toInstance(resourceConfig)
      bind[SchemaProvider].toInstance(new AionConfig(classOf[ApplicationSpec].getResourceAsStream(s"schema-${name}.yml")))
      bind[DataSource].toInstance(dataSource)
    }
  }

  def namedApplication(name: String, testModule: Option[TestModule] = None) = {
    import net.codingwell.scalaguice.InjectorExtensions._

    val tModule = testModule match {
      case Some(m) => m
      case None => new TestModule(name)
    }

    val injector = Guice.createInjector(
      TypesafeConfigModule.fromConfig(ApplicationSpec.namedConfig(name)),
      JacksonModule,
      Slf4jLoggerModule,
      AionResourceModule,
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

  implicit class TestResult(val response: Response) {
    def shouldBeOfFamily(family: Response.Status.Family) {
      response.getStatusInfo.getFamily shouldBe family
    }
  }

  "An Application" should "be initializable with minimal configuration" in {
    val uut = defaultApplication
    uut should not be (null)
  }

  it should s"register only hard coded resources with no objects" in {
    val f = defaultFixture
    val uut = f.app
    f.testModule.resourceConfig.getClasses should not be (null)
    f.testModule.resourceConfig.getSingletons should not be (null)

    // The extra one here is for JacksonFeature
    f.testModule.resourceConfig.resourceCount shouldBe (f.app.hardCodedResources.size + 1)
  }

  it should "register resources of complete schema" in {
    val f = namedFixture("complete")
    val uut = f.app
    val resourceConfig = f.testModule.resourceConfig
    val registeredResources = resourceConfig.getResources
    val resourcePaths = registeredResources.map(r => (r.getPath, r)).toMap

    val expectedPaths = Seq(
      "/foo/single_partition/{partition}",
      "/foo/double_partition/{partition}/{range}",
      "/foo/no_partition"
    )
    for (p <- expectedPaths) {
      resourcePaths.contains(p) shouldBe true
    }
  }

  it should "respond to schema requests" in {
    val f = namedFixture("complete")

    val result: Response = f.test.target("/schema").request().get()
    result shouldBeOfFamily SUCCESSFUL

    f.test.tearDown
  }

  it should "respond to version requests" in {
    val f = defaultFixture

    val result: Response = f.test.target("/version").request.get
    result shouldBeOfFamily SUCCESSFUL
    result.getMediaType shouldBe TEXT_PLAIN_TYPE 
    f.test.tearDown
  }

  it should "ask DataStore to initialize schema on startup" in {
    val f = defaultFixture
    verify(f.testModule.dataSource).initializeSchema(anyObject())
  }

  "The schema resource" should "report accurate schema information" in {
    val f = namedFixture("complete")
    val result: Response = f.test.target("/schema").request().get()
    result shouldBeOfFamily SUCCESSFUL

    val jsonResult = result.readEntity(classOf[String])
    val schemaMap = f.app.mapper.readValue(jsonResult, classOf[Map[String, Map[String, String]]])
    schemaMap shouldEqual Map (
      "foo" -> Map (
        "partition" -> "text",
        "range" -> "text",
        "time" -> "timeuuid",
        "data" -> "blob",
        "datam" -> "map<text,blob>"
      )
    )
  }

  "The resource resource" should "ask the DataSource for data on HTTP GET on an index with multiple partition keys" in {
    import java.time.Instant
    import java.time.temporal.ChronoUnit._

    val f = namedFixture("complete")

    // given
    f.testModule.setupTestDataTypes
    given(f.testModule.dataSource.executeQuery(anyObject(), anyObject(), anyObject(), anyObject())).willReturn(Seq())

    // when
    val now = Instant.now
    val end = now.plus(1, HOURS)
    val result: Response = f.test.target(s"/foo/single_partition/somePartition").queryParam("from", now.toString).queryParam("to", end.toString).request().get()

    // then
    result.getStatus shouldBe 200
    verify(f.testModule.dataSource).executeQuery(
      anyObject(),
      anyObject(),
      anyObject(),
      MockitoMatchers.eq(Map("partition" -> "somePartition"))
    ) // TODO: better matching of the QueryStrategy
  }

  it should "ask the DataSource for data on HTTP GET on a full index with range keys" in {
    import java.time.Instant
    import java.time.temporal.ChronoUnit._

    val f = namedFixture("complete")

    // given
    f.testModule.setupTestDataTypes
    given(f.testModule.dataSource.executeQuery(anyObject(), anyObject(), anyObject(), anyObject())).willReturn(Seq())

    // when
    val now = Instant.now
    val end = now.plus(1, HOURS)
    val result: Response = f.test.target(s"/foo/double_partition/somePartition/someRange").queryParam("from", now.toString).queryParam("to", end.toString).request().get()

    // then
    result.getStatus shouldBe 200
    verify(f.testModule.dataSource).executeQuery(
      anyObject(),
      anyObject(),
      anyObject(),
      MockitoMatchers.eq(Map("partition" -> "somePartition", "range" -> "someRange"))
    ) // TODO: better matching of the QueryStrategy
  }

  it should "return 404 for data on HTTP GET on a full index without partition keys" in {
    import java.time.Instant
    import java.time.temporal.ChronoUnit._

    val f = namedFixture("complete")

    // given
    f.testModule.setupTestDataTypes
    given(f.testModule.dataSource.executeQuery(anyObject(), anyObject(), anyObject(), anyObject())).willReturn(Seq())

    // when
    val now = Instant.now
    val end = now.plus(1, HOURS)
    val result: Response = f.test.target("/foo/single_partition").queryParam("from", now.toString).queryParam("to", now.toString).request().get()

    // then
    result.getStatus shouldBe 404
  }

  it should "ask the DataSource for data on HTTP GET with no partition / range keys" in {
    import java.time.Instant
    import java.time.temporal.ChronoUnit._

    val f = namedFixture("complete")

    // given
    f.testModule.setupTestDataTypes
    given(f.testModule.dataSource.executeQuery(anyObject(), anyObject(), anyObject(), anyObject())).willReturn(Seq())

    // when
    val now = Instant.now
    val end = now.plus(1, HOURS)
    val result: Response = f.test.target(s"/foo/no_partition").queryParam("from", now.toString).queryParam("to", end.toString).request().get()

    // then
    result.getStatus shouldBe 200
    verify(f.testModule.dataSource).executeQuery(anyObject(), anyObject(), anyObject(), MockitoMatchers.eq(Map())) // TODO: better matching of the QueryStrategy

    f.test.tearDown
  }

  "The index resource" should "insert into the DataSource on HTTP PUT with no range keys" in {
    import com.datastax.driver.core.utils.UUIDs
    import javax.ws.rs.client.Entity

    val f = namedFixture("complete")

    f.testModule.setupTestDataTypes

    val result: Response = f.test.target(s"/foo").request().post(Entity.json(s"""{
      "partition": "somePartition",
      "time": "${UUIDs.timeBased}",
      "data": ""
    }"""))

    result.getStatus shouldBe 201
    verify(f.testModule.dataSource).insertQuery(anyObject(), anyObject())

    f.test.tearDown
  }

  it should "return 400 bad request on bad JSON input" in {
    import javax.ws.rs.client.Entity

    val f = namedFixture("complete")

    f.testModule.setupTestDataTypes

    val result: Response = f.test.target("/foo").request().post(Entity.json("not really json"))

    result.getStatus shouldBe 400
  }
}
