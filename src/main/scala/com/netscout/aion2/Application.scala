package com.netscout.aion2

import com.typesafe.config.{Config, ConfigFactory}

import org.glassfish.jersey.server.ResourceConfig

import scala.collection.JavaConversions._

class ApplicationWrapper extends Application(ConfigFactory.load())

class Application(val config: Config) extends ResourceConfig {
  import com.fasterxml.jackson.databind.ObjectMapper
  import com.fasterxml.jackson.module.scala.DefaultScalaModule
  import com.netscout.aion2.model.{AionObjectConfig, AionIndexConfig}
  import com.netscout.aion2.source.CassandraDataSource
  import com.typesafe.config.ConfigException

  val builtinSchemaProviders: Iterable[SchemaProvider] = Seq(new AionConfig(config))
  var dataSource = new CassandraDataSource(getOptionalConfig("dataSource"))

  // Initialize Jackson for JSON parsing
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
   * Gets an optional configuration value
   */
  private[aion2] def getOptionalConfig(key: String) = {
    try {
      Some(config.getConfig(getConfigKey(key)))
    } catch {
      case (e: ConfigException.Missing) => None
    }
  }

  /**
   * Prepends the configuration prefix to the key to produce a
   * fully pathed configuration key
   */
  private[aion2] def getConfigKey(key: String) = {
    "com.netscout.aion2." ++ key
  }

  implicit class AionIndexResource(val index: AionIndexConfig) {
    /**
     * Gets the path for the JAX-RS resource for this index
     */
    def resourcePath = {
      val partitionPathKeys = index.partition.map(p => s"{$p}")
      (Seq(index.name) ++ partitionPathKeys) mkString "/"
    }
  }

  implicit class AionObjectWithResources(val obj: AionObjectConfig) {
    import javax.ws.rs.core.{Response, StreamingOutput}
    import javax.ws.rs.core.MediaType._

    /**
     * Dynamically creates JAX-RS resources for a given Aion object description
     *
     * @param obj the aion object for which to build resources
     */
    def resources = obj.indices.map(index => {
      import javax.ws.rs.container.ContainerRequestContext
      import org.glassfish.jersey.process.Inflector
      import org.glassfish.jersey.server.model.Resource

      // Jersey provides a programmatic API for building JAX-RS resources
      // @see [[org.glassfish.jersey.server.model.Resource.Builder
      val resourceBuilder = Resource.builder()
      resourceBuilder.path(index.resourcePath)

      resourceBuilder.addMethod("GET").produces(APPLICATION_JSON).handledBy(new Inflector[ContainerRequestContext, Response] {
        val splitStrategy = index.split.strategy.strategy

        override def apply(request: ContainerRequestContext) = {
          val info = request.getUriInfo
          val queryParameters = info.getQueryParameters

          val queryStrategy = splitStrategy.strategyForQuery(info.getQueryParameters)

          val results = dataSource.executeQuery(obj, index, queryStrategy, info.getPathParameters.mapValues(_.head).toMap).map(_.toMap)
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
  }

  /**
   * Registers resources associated with the given schema provider
   *
   * @param schemaProvider the schema provider to register
   */
  def registerSchemaProvider(schemaProvider: SchemaProvider) {
    val schemata = schemaProvider.schema
    val resourceLists = schemata.map(_.resources)

    // If we have 0 resources, reduce() will cause an error
    if (resourceLists.size > 0) {
      registerResources(resourceLists.reduce(_++_))
    }
  }

  // This registers all the resources found by the default schema providers
  builtinSchemaProviders.foreach(registerSchemaProvider(_))
}
