package com.netscout.aion2.resources

import com.google.inject.Inject
import com.netscout.aion2.model.AionObjectConfig

import javax.ws.rs._
import javax.ws.rs.core.{Response, StreamingOutput}
import javax.ws.rs.core.MediaType._

import scala.collection.JavaConversions._

@Path("/schema")
class Schema @Inject() {
  val schema: scala.collection.mutable.Set[AionObjectConfig] = scala.collection.mutable.Set.empty

  /**
   * Registers a new schema with this resource
   */
  def registerSchema(newSchema: Set[AionObjectConfig]) {
    schema ++= newSchema
  }

  private def schemaMap = schema.map(obj => {
    (obj.name, obj)
  }).toMap

  private def fieldsSchema = schema.map(obj => {
    (obj.name, obj.fields)
  }).toMap

  @GET
  @Produces(Array(APPLICATION_JSON))
  def getSchema = {
    import scala.collection.JavaConverters._

    fieldsSchema.asJava
  }

  @GET
  @Path("{name}")
  @Produces(Array(APPLICATION_JSON))
  def getObject(@PathParam("name") name: String) = {
    import javax.ws.rs.core.Response.Status._

    schemaMap.get(name).getOrElse(throw new WebApplicationException(NOT_FOUND))
  }
}
