package com.netscout.aion2

import org.glassfish.jersey.server.ResourceConfig

import scala.collection.JavaConversions._

class Application extends ResourceConfig {
  import com.netscout.aion2.model.{AionObjectConfig, AionIndexConfig}
  import com.typesafe.config.ConfigFactory

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

      val resourceBuilder = Resource.builder()
      resourceBuilder.path(index.resourcePath)

      resourceBuilder.addMethod("GET").produces("text/plain").handledBy(new Inflector[ContainerRequestContext, String] {
        val splitStrategy = index.split.strategy.strategy

        override def apply(request: ContainerRequestContext) = {
          val info = request.getUriInfo
          val queryParameters = info.getQueryParameters

          val queryStrategy = splitStrategy.strategyForQuery(info.getQueryParameters)
          s"minimum: ${queryStrategy.minimum}\nmaximum: ${queryStrategy.maximum}\npartialRows: ${queryStrategy.partialRows}\nfullRows: ${queryStrategy.fullRows}"
        }
      })
      resourceBuilder.build()
    })
  }

  val config = ConfigFactory.load()

  val schemaProviders: Iterable[SchemaProvider] = Seq(new AionConfig(config))

  val resources = {
    val schemata = schemaProviders.map(_.schema).reduce(_++_)
    schemata.map(_.resources).reduce(_++_).toArray
  }

  registerResources(resources : _*)
}
