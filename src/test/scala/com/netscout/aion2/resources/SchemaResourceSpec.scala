package com.netscout.aion2.resources

import com.google.inject.Guice
import com.netscout.aion2.ResourceConfigUtils
import com.netscout.aion2.inject._
import com.netscout.aion2.model._

import javax.ws.rs.core.Response
import javax.ws.rs.core.MediaType._

import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.JerseyTest
import org.scalatest._

class SchemaResourceSpec extends FlatSpec with Matchers {
  def schemaResource = {
    import net.codingwell.scalaguice.InjectorExtensions._

    val injector = Guice.createInjector(Slf4jLoggerModule, AionResourceModule)
    injector.instance[Schema]
  }

  def resourceConfig(resource: Schema) = {
    import org.glassfish.jersey.jackson.JacksonFeature

    val cfg = new ResourceConfig
    cfg.register(classOf[JacksonFeature])
    ResourceConfigUtils.register(cfg, resource)
    cfg
  }

  class SchemaResourceJerseyTest (
    val app: ResourceConfig
  ) extends JerseyTest(app) {
    setUp()
  }

  def fixture =
    new {
      val resource = schemaResource
      val test = new SchemaResourceJerseyTest(resourceConfig(resource))
    }

  private def testObject = {
    import scala.collection.JavaConversions._

    val obj = new AionObjectConfig
    obj.name = "some_object"
    obj.fields = Map(
      "key" -> "text",
      "value" -> "double"
    )
    val index = {
      val index = new AionIndexConfig
      index.name = "some_index"
      index.partition = Seq("key")
      index.split = new AionSplitKeyConfig
      index.split.column = "key"
      index.split.strategy = new AionSplitStrategyConfig
      index.split.strategy.name = "none"
      index.split.strategy.config = new java.util.HashMap[String, String]
      index.clustering = new AionClusteringConfig
      index.clustering.field = "key"
      index.clustering.order = "DESC"
      index
    }
    obj.indices = Seq(index)
    obj
  }

  /**
   * Convenience method for checking that a response was successful and thinks that it returns
   * the JSON content type.
   */
  private def responseShouldBeSuccessfulJSON(response: Response) {
    response.getStatusInfo.getFamily shouldBe Response.Status.Family.SUCCESSFUL
    response.getMediaType shouldBe APPLICATION_JSON_TYPE
  }

  "A schema resource" should "successfully return JSON for the schema endpoint with objects" in {
    val f = fixture
    f.resource.registerSchema(Set(testObject))

    val response: Response = f.test.target("/schema").request.get

    responseShouldBeSuccessfulJSON(response)
  }

  it should "successfully return JSON for the schema endpoint with no objects" in {
    val f = fixture

    val response: Response = f.test.target("/schema").request.get

    responseShouldBeSuccessfulJSON(response)
  }

  it should "successfully return JSON for objects that exist" in {
    val f = fixture
    f.resource.registerSchema(Set(testObject))

    val response: Response = f.test.target("/schema/some_object").request.get
    responseShouldBeSuccessfulJSON(response)
  }
  
  it should "return 404 for objects that don't exist" in {
    val f = fixture
    val response: Response = f.test.target("/schema/should_not_exist").request.get
    response.getStatus shouldBe 404
  }
}
