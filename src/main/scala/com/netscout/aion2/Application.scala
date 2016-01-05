package com.netscout.aion2

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.racc.tscg.TypesafeConfigModule
import com.google.inject.{AbstractModule, Guice, Inject}
import com.netscout.aion2.except._
import com.netscout.aion2.model.DataSource
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
    new ApplicationWrapperModule(this))

  val realApplication = injector.instance[Application]
}

class Application @Inject() (
  configSchemaProvider: SchemaProvider,
  val dataSource: DataSource,
  val mapper: ObjectMapper,
  val resourceConfig: ResourceConfig
) {
  import com.netscout.aion2.model.{AionObjectConfig, AionIndexConfig}
  import com.netscout.aion2.source.CassandraDataSource
  import com.typesafe.config.ConfigException

  val builtinSchemaProviders: Iterable[SchemaProvider] = Seq(configSchemaProvider)

  implicit class AionIndexResource(val index: AionIndexConfig) {
    /**
     * Gets the path for the element JAX-RS resources for this index
     */
    def resourcePath = {
      val partitionPathKeys = index.partition.map(p => s"{$p}")
      val path = (Seq(index.name) ++ partitionPathKeys) mkString "/"
      val rangeKeysParameter = if (index.range.size > 0) {
        s"{rangeKeys: ((/([\\w\\.\\d\\-%]+)){1,${index.range.size}})?}"
      } else {
        ""
      }

      "/" ++ path ++ rangeKeysParameter
    }

    /**
     * Gets a path for the root JAX-RS resource for this index
     */
    def indexPath = s"/${index.name}"
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

      val splitStrategy = index.split.strategy.strategy

      // Jersey provides a programmatic API for building JAX-RS resources
      // @see [[org.glassfish.jersey.server.model.Resource.Builder
      val resourceBuilder = Resource.builder()
      resourceBuilder.path(index.resourcePath)

      resourceBuilder.addMethod("GET").produces(APPLICATION_JSON).handledBy(new Inflector[ContainerRequestContext, Response] {
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

      val indexResourceBuilder = Resource.builder()
      indexResourceBuilder.path(index.indexPath)

      indexResourceBuilder.addMethod("POST").produces(APPLICATION_JSON).handledBy(new Inflector[ContainerRequestContext, Response] {
        override def apply(request: ContainerRequestContext) = {
          import com.fasterxml.jackson.core.JsonParseException
          import com.fasterxml.jackson.databind.{JsonMappingException, JsonNode}
          import javax.ws.rs.core.Response.Status._

          try {
            val values = mapper.readTree(request.getEntityStream)
            val newValues = values.fieldNames.map(fieldName => {
              val typeName = Option(obj.fields.get(fieldName)) match {
                case Some(name) => name
                case None => throw new IllegalQueryException(s"Field ${fieldName} not included in object description for index ${index.name}")
              }
              val jsonNode = Option(values.findValue(fieldName)).getOrElse(throw new Exception("This shouldn't happen if Jackson returns consistent data"))
              val newV = mapper.treeToValue(jsonNode, dataSource.classOfType(typeName.toString))
              (fieldName, newV.asInstanceOf[AnyRef])
            }).toMap
            val maybeSplitKeyValue = for {
              realValue <- newValues.get(index.split.column)
              roundedValue <- Some(splitStrategy.rowKey(realValue.asInstanceOf[AnyRef]))
            } yield roundedValue
            val splitKeyValue = maybeSplitKeyValue match {
              case Some(x) => x
              case None => throw new IllegalQueryException("The split key value must be present for an insert")
            }
            dataSource.insertQuery(obj, index, newValues, splitKeyValue.asInstanceOf[AnyRef])
            Response.status(CREATED).build()
          } catch {
            case (jme: JsonMappingException) => throw new IllegalQueryException("Error mapping JSON input to values", jme)
            case (jpe: JsonParseException) => throw new IllegalQueryException("Error parsing JSON input", jpe)
          }
        }
      })

      Set(resourceBuilder.build(), indexResourceBuilder.build())
    }).reduce(_++_)
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

    val resourceLists = schemata.map(_.resources)

    // If we have 0 resources, reduce() will cause an error
    if (resourceLists.size > 0) {
      val resourceList = resourceLists.reduce(_++_)
      resourceConfig.registerResources(resourceList)
    }
  }

  // This registers all the resources found by the default schema providers
  builtinSchemaProviders.foreach(registerSchemaProvider(_))
}
