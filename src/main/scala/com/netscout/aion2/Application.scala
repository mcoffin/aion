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

  val schemaProviders: Iterable[SchemaProvider] = Seq(new AionConfig(config))
  val dataSource = new CassandraDataSource(getOptionalConfig("dataSource"))

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

      resourceBuilder.addMethod("GET").produces("application/json").handledBy(new Inflector[ContainerRequestContext, String] {
        val splitStrategy = index.split.strategy.strategy

        override def apply(request: ContainerRequestContext) = {
          val info = request.getUriInfo
          val queryParameters = info.getQueryParameters

          val queryStrategy = splitStrategy.strategyForQuery(info.getQueryParameters)
          val results = dataSource.executeQuery(obj, index, queryStrategy)
          mapper writeValueAsString results
        }
      })
      resourceBuilder.build()
    })
  }

  val resources = {
    import org.glassfish.jersey.server.model.Resource

    // Combine all the schemas from all the schema providers
    val schemata = schemaProviders.map(_.schema).reduce(_++_)

    // Combine all the resouces from all of the schemata
    val resources = schemata.map(_.resources)
    if (resources.size > 0) {
      resources.reduce(_++_).toArray
    } else {
      Array[Resource]()
    }
  }

  // Register all the generated resources with this JAX-RS application
  registerResources(resources : _*)
}
