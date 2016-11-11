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

/**
 * Small wrapper class to bootstrap the Application
 *
 * This will hopefully be replaced by an OSGi bootstrap solution
 * if we want to do dynamic location of plugins / DataSources / SplitStrategies / etc.
 */
class ApplicationWrapper extends ResourceConfig {
  import com.netscout.aion2.inject._
  import net.codingwell.scalaguice.InjectorExtensions._

  /**
   * Small guice module for binding
   * [[org.glassfish.jersey.server.ResourceConfig]] to this wrapper class so
   * that the wrapper class can act as a JAX-RS
   * [[javax.ws.rs.core.Application]].
   */
  class ApplicationWrapperModule (
    val wrapper: ApplicationWrapper
  ) extends AbstractModule with ScalaModule {
    override def configure {
      bind[ResourceConfig].toInstance(wrapper)
    }
  }

  val injector = Guice.createInjector(
    TypesafeConfigModule.fromConfigWithPackage(ConfigFactory.load, "com.netscout.aion2"),
    SystemPropertiesModule,
    SchemaProviderModule,
    DataSourceModule,
    JacksonModule,
    Slf4jLoggerModule,
    AionResourceModule,
    new ApplicationWrapperModule(this))

  val realApplication = injector.instance[Application]
}

/**
 * Root class of Aion.
 *
 * Instantiating this class will register all of its generated resources with the
 * provided ResourceConfig
 *
 * @param configSchemaProvider the schema provider to use
 * @param mapper JSON object mapper
 * @param resourceConfig The ResourceConfig instance with which to register JAX-RS resources
 * @param schemaResource The schema resource (created by guice)
 * @param versionResource The version resource (created by guice)
 */
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
  import org.glassfish.jersey.jackson.JacksonFeature

  /**
   * List of built-in schema providers
   */
  val builtinSchemaProviders: Iterable[SchemaProvider] = Seq(configSchemaProvider)

  /**
   * Convenience methods for working in the context of an AionObjectConfig
   */
  implicit class AionObjectWithResources(val obj: AionObjectConfig) {
    import com.fasterxml.jackson.databind.JsonNode
    import javax.ws.rs.core.{Response, StreamingOutput}
    import javax.ws.rs.core.MediaType._

    /**
     * The resource path for this object
     */
    val resourcePath = s"/${obj.name}"

    private def jsonToDataSourceObject(fieldName: String, jsonNode: JsonNode): AnyRef = {
      val typeName = Option(obj.fields.get(fieldName)).getOrElse(throw new IllegalQueryException(s"Field ${fieldName} not included in object description for object ${obj.name}"))
      mapper.treeToValue(jsonNode, dataSource.classOfType(typeName)).asInstanceOf[AnyRef]
    }

    /**
     * The resource path for a given index inside of this object
     */
    def indexResourcePath(index: AionIndexConfig) = {
      val partitionPathKeys = index.partition.map(p => s"{$p}")
      (Seq(resourcePath, index.name) ++ partitionPathKeys) mkString "/"
    }

    /**
     * Builds a JAX-RS resource for the collection represented by this object
     */
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
            // First read the JSON AST without mapping any objects
            val values = mapper.readTree(request.getEntityStream)

            // Then go through and map each object to the class desired by the dataSource
            val mappedValues = values.fieldNames.map(fieldName => {
              // This shouldn't return null since we're mapping over the values that the Jackson JsonNode told us that it had
              val jsonNode = Option(values.findValue(fieldName)).getOrElse(throw new RuntimeException("This shouldn't happen if Jackson returns consistent data"))
              (fieldName, jsonToDataSourceObject(fieldName, jsonNode))
            }).toMap

            // Lastly, perform the insert query and respond appropriately
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

    /**
     * Builds JAX-RS resources for the elements of each index in this object
     */
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

          // Because technically path parameters can have multiple
          // instantiations, getPathParameters returns a MultivaluedMap.
          // Since we only want a single value, we just map over each of the
          // value lists and return the first one.
          val pathParameters = info.getPathParameters.mapValues(_.head).toMap
          val mappedPathParameters = pathParameters.map(_ match {
            case (k, v) => {
              val jsonNode = mapper.readTree(mapper.writeValueAsBytes(v))
              (k, jsonToDataSourceObject(k, jsonNode))
            }
          })

          val queryStrategy = splitStrategy.strategyForQuery(queryParameters)

          // Actually execute the query against the dataSource
          val results = dataSource.executeQuery(obj, index, queryStrategy, mappedPathParameters).map(_.toMap)

          // We use a streaming output here so that we don't have to store the
          // entire result of the JSON serialization in memory while we're
          // writing the output
          val stream = new StreamingOutput() {
            import java.io.OutputStream

            override def write(output: OutputStream) {
              mapper.writeValue(output, results)
            }
          }

          // Finally build the JAX-RS response
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

  // Set up Jackson for JAX-RS (jersey impl)
  resourceConfig.register(classOf[JacksonFeature])

  // This registers all the resources found by the default schema providers
  builtinSchemaProviders.foreach(registerSchemaProvider(_))

  // Now register all hard-coded resources (for metadata like /schema and /version)
  val hardCodedResources = Seq(schemaResource, versionResource)
  hardCodedResources.foreach(ResourceConfigUtils.register(resourceConfig, _))
}
