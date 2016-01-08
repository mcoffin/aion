package com.netscout.aion2

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.racc.tscg.TypesafeConfigModule
import com.google.inject.{AbstractModule, Guice, Inject}
import com.netscout.aion2.except._
import com.netscout.aion2.model.DataSource
import com.netscout.aion2.resources._
import com.typesafe.config.ConfigFactory

import javax.ws.rs.core.{Application => JAXRSApplication}

import net.codingwell.scalaguice.ScalaModule

import org.glassfish.jersey.server.ResourceConfig

import scala.collection.JavaConversions._

class ApplicationWrapper extends ResourceConfig {
  import com.netscout.aion2.inject._
  import net.codingwell.scalaguice.InjectorExtensions._

  class ApplicationWrapperModule (
    val wrapper: ApplicationWrapper
  ) extends AbstractModule with ScalaModule {
    override def configure {
      bind[ResourceConfig].toInstance(wrapper)
    }
  }

  val injector = Guice.createInjector(
    TypesafeConfigModule.fromConfig(ConfigFactory.load),
    SchemaProviderModule,
    DataSourceModule,
    JacksonModule,
    Slf4jLoggerModule,
    AionResourceModule,
    new ApplicationWrapperModule(this))

  val realApplication = injector.instance[Application]
}

class Application @Inject() (
  configSchemaProvider: SchemaProvider,
  val dataSource: DataSource,
  val mapper: ObjectMapper,
  val resourceConfig: ResourceConfig,
  val schemaResource: Schema,
  val versionResource: VersionResource
) {
  import com.netscout.aion2.model.{AionObjectConfig, AionIndexConfig}
  import com.netscout.aion2.source.CassandraDataSource
  import com.typesafe.config.ConfigException

  val builtinSchemaProviders: Iterable[SchemaProvider] = Seq(configSchemaProvider)

  implicit class AionObjectWithResources(val obj: AionObjectConfig) {
    import com.fasterxml.jackson.databind.JsonNode
    import javax.ws.rs.core.{Response, StreamingOutput}
    import javax.ws.rs.core.MediaType._

    val resourcePath = s"/${obj.name}"

    private def jsonToDataSourceObject(fieldName: String, jsonNode: JsonNode): AnyRef = {
      val typeName = Option(obj.fields.get(fieldName)).getOrElse(throw new IllegalQueryException(s"Field ${fieldName} not included in object description for object ${obj.name}"))
      mapper.treeToValue(jsonNode, dataSource.classOfType(typeName)).asInstanceOf[AnyRef]
    }

    def indexResourcePath(index: AionIndexConfig) = {
      val partitionPathKeys = index.partition.map(p => s"{$p}")
      (Seq(resourcePath, index.name) ++ partitionPathKeys) mkString "/"
    }

    def collectionResource = {
      import javax.ws.rs.container.ContainerRequestContext
      import org.glassfish.jersey.process.Inflector
      import org.glassfish.jersey.server.model.Resource

      val resourceBuilder = Resource.builder()
      resourceBuilder.path(resourcePath)

      resourceBuilder.addMethod("POST").produces(APPLICATION_JSON).handledBy(new Inflector[ContainerRequestContext, Response] {
        override def apply(request: ContainerRequestContext) = {
          import com.fasterxml.jackson.core.JsonParseException
          import com.fasterxml.jackson.databind.{JsonMappingException, JsonNode}
          import javax.ws.rs.core.Response.Status._

          try {
            val values = mapper.readTree(request.getEntityStream)
            val mappedValues = values.fieldNames.map(fieldName => {
              val jsonNode = Option(values.findValue(fieldName)).getOrElse(throw new RuntimeException("This shouldn't happen if Jackson returns consistent data"))
              (fieldName, jsonToDataSourceObject(fieldName, jsonNode))
            }).toMap
            dataSource.insertQuery(obj, mappedValues)
            Response.status(CREATED).build()
          } catch {
            case (jme: JsonMappingException) => throw new IllegalQueryException("Error mapping JSON input to objects", jme)
            case (jpe: JsonParseException) => throw new IllegalQueryException("Error parsing JSON input", jpe)
          }
        }
      })
      resourceBuilder.build()
    }

    def indexResources = obj.indices.map(index => {
      import javax.ws.rs.container.ContainerRequestContext
      import org.glassfish.jersey.process.Inflector
      import org.glassfish.jersey.server.model.Resource

      val resourceBuilder = Resource.builder()
      resourceBuilder.path(indexResourcePath(index))

      val splitStrategy = index.split.strategy.strategy

      resourceBuilder.addMethod("GET").produces(APPLICATION_JSON).handledBy(new Inflector[ContainerRequestContext, Response] {
        override def apply(request: ContainerRequestContext) = {
          val info = request.getUriInfo
          val queryParameters = info.getQueryParameters
          val pathParameters = info.getPathParameters.mapValues(_.head).toMap
          val mappedPathParameters = pathParameters.map(_ match {
            case (k, v) => {
              val jsonNode = mapper.readTree(mapper.writeValueAsBytes(v))
              (k, jsonToDataSourceObject(k, jsonNode))
            }
          })

          val queryStrategy = splitStrategy.strategyForQuery(queryParameters)

          val results = dataSource.executeQuery(obj, index, queryStrategy, mappedPathParameters).map(_.toMap)
          val stream = new StreamingOutput() {
            import java.io.OutputStream

            override def write(output: OutputStream) {
              mapper.writeValue(output, results)
            }
          }
          Response.ok(stream).build()
        }
      })
      resourceBuilder.build()
    })

    /**
     * Dynamically creates JAX-RS resources for this Aion object description
     */
    def resources = indexResources :+ collectionResource
  }

  /**
   * Registers resources associated with the given schema provider
   *
   * @param schemaProvider the schema provider to register
   */
  def registerSchemaProvider(schemaProvider: SchemaProvider) {
    val schemata = schemaProvider.schema

    // Initialize the datasource with the new schema
    dataSource.initializeSchema(schemata)

    // Register the new schema with the schemaResource
    schemaResource.registerSchema(schemata)

    val resourceLists = schemata.map(_.resources)

    // If we have 0 resources, reduce() will cause an error
    if (resourceLists.size > 0) {
      val resourceList = resourceLists.reduce(_++_)
      resourceConfig.registerResources(resourceList : _*)
    }
  }

  // This registers all the resources found by the default schema providers
  builtinSchemaProviders.foreach(registerSchemaProvider(_))

  // Now register all hard-coded resources (for metadata like /schema and /version)
  val hardCodedResources = Seq(schemaResource, versionResource)
  hardCodedResources.foreach(ResourceConfigUtils.register(resourceConfig, _))
}
