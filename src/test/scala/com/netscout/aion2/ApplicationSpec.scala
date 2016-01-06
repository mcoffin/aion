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

      doReturn(classOf[String]).when(dataSource).classOfType("text")
      doReturn(classOf[UUID]).when(dataSource).classOfType("timeuuid")
      doReturn(classOf[Array[Byte]]).when(dataSource).classOfType("blob")
    }

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
    f.testModule.resourceConfig.resourceCount shouldBe f.app.hardCodedResources.size
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
    val fullResourcePath = "/foo/{partition}{rangeKeys : ((/([\\w\\.\\d\\-%]+)){1,1})?}"
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
        "data" -> "blob"
      ),
      "bar" -> Map (
        "partition" -> "text",
        "time" -> "timeuuid",
        "data" -> "blob"
      ),
      "no_partition" -> Map (
        "time" -> "timeuuid",
        "data" -> "blob"
      )
    )
  }

  "The resource resource" should "ask the DataSource for data on HTTP GET with no range keys" in {
    import java.time.Instant
    import java.time.temporal.ChronoUnit._

    val f = namedFixture("complete")

    // given
    f.testModule.setupTestDataTypes
    given(f.testModule.dataSource.executeQuery(anyObject(), anyObject(), anyObject(), anyObject(), anyObject())).willReturn(Seq())

    // when
    val now = Instant.now
    val end = now.plus(1, HOURS)
    val result: Response = f.test.target(s"/bar/somePartition").queryParam("from", now.toString).queryParam("to", end.toString).request().get()

    // then
    result.getStatus shouldBe 200
    verify(f.testModule.dataSource).executeQuery(
      anyObject(),
      anyObject(),
      anyObject(),
      MockitoMatchers.eq(Map("partition" -> "somePartition")),
      MockitoMatchers.eq(Map())
    ) // TODO: better matching of the QueryStrategy

    f.test.tearDown
  }

  it should "ask the DataSource for data on HTTP GET on a full index with no range keys" in {
    import java.time.Instant
    import java.time.temporal.ChronoUnit._

    val f = namedFixture("complete")

    // given
    f.testModule.setupTestDataTypes
    given(f.testModule.dataSource.executeQuery(anyObject(), anyObject(), anyObject(), anyObject(), anyObject())).willReturn(Seq())

    // when
    val now = Instant.now
    val end = now.plus(1, HOURS)
    val result: Response = f.test.target(s"/foo/somePartition").queryParam("from", now.toString).queryParam("to", end.toString).request().get()

    // then
    result.getStatus shouldBe 200
    verify(f.testModule.dataSource).executeQuery(
      anyObject(),
      anyObject(),
      anyObject(),
      MockitoMatchers.eq(Map("partition" -> "somePartition")),
      MockitoMatchers.eq(Map())
    ) // TODO: better matching of the QueryStrategy
  }

  it should "ask the DataSource for data on HTTP GET on a full index with range keys" in {
    import java.time.Instant
    import java.time.temporal.ChronoUnit._

    val f = namedFixture("complete")

    // given
    f.testModule.setupTestDataTypes
    given(f.testModule.dataSource.executeQuery(anyObject(), anyObject(), anyObject(), anyObject(), anyObject())).willReturn(Seq())

    // when
    val now = Instant.now
    val end = now.plus(1, HOURS)
    val result: Response = f.test.target(s"/foo/somePartition/someRange").queryParam("from", now.toString).queryParam("to", end.toString).request().get()

    // then
    result.getStatus shouldBe 200
    verify(f.testModule.dataSource).executeQuery(
      anyObject(),
      anyObject(),
      anyObject(),
      MockitoMatchers.eq(Map("partition" -> "somePartition")),
      MockitoMatchers.eq(Map("range" -> "someRange"))
    ) // TODO: better matching of the QueryStrategy
  }

  it should "return 404 for data on HTTP GET on a full index without partition keys" in {
    import java.time.Instant
    import java.time.temporal.ChronoUnit._

    val f = namedFixture("complete")

    // given
    f.testModule.setupTestDataTypes
    given(f.testModule.dataSource.executeQuery(anyObject(), anyObject(), anyObject(), anyObject(), anyObject())).willReturn(Seq())

    // when
    val now = Instant.now
    val end = now.plus(1, HOURS)
    val result: Response = f.test.target("/foo").queryParam("from", now.toString).queryParam("to", now.toString).request().get()

    // then
    result.getStatus shouldBe 405
  }

  it should "ask the DataSource for data on HTTP GET with no partition / range keys" in {
    import java.time.Instant
    import java.time.temporal.ChronoUnit._

    val f = namedFixture("complete")

    // given
    f.testModule.setupTestDataTypes
    given(f.testModule.dataSource.executeQuery(anyObject(), anyObject(), anyObject(), anyObject(), anyObject())).willReturn(Seq())

    // when
    val now = Instant.now
    val end = now.plus(1, HOURS)
    val result: Response = f.test.target(s"/no_partition").queryParam("from", now.toString).queryParam("to", end.toString).request().get()

    // then
    result.getStatus shouldBe 200
    verify(f.testModule.dataSource).executeQuery(anyObject(), anyObject(), anyObject(), MockitoMatchers.eq(Map()), MockitoMatchers.eq(Map())) // TODO: better matching of the QueryStrategy

    f.test.tearDown
  }

  "The index resource" should "insert into the DataSource on HTTP PUT with no range keys" in {
    import com.datastax.driver.core.utils.UUIDs
    import javax.ws.rs.client.Entity

    val f = namedFixture("complete")

    f.testModule.setupTestDataTypes

    val result: Response = f.test.target(s"/bar").request().post(Entity.json(s"""{
      "partition": "somePartition",
      "time": "${UUIDs.timeBased}",
      "data": ""
    }"""))

    result.getStatus shouldBe 201
    verify(f.testModule.dataSource).insertQuery(anyObject(), anyObject(), anyObject(), anyObject())

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
