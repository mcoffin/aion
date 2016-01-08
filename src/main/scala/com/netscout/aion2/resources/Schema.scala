package com.netscout.aion2.resources

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.Inject
import com.netscout.aion2.model.AionObjectConfig

import javax.ws.rs._
import javax.ws.rs.core.{Response, StreamingOutput}
import javax.ws.rs.core.MediaType._

import scala.collection.JavaConversions._

@Path("/schema")
class Schema @Inject() (
  val mapper: ObjectMapper
) {
  val schema: scala.collection.mutable.Set[AionObjectConfig] = scala.collection.mutable.Set.empty

  /**
   * Registers a new schema with this resource
   */
  def registerSchema(newSchema: Set[AionObjectConfig]) {
    schema ++= newSchema
  }

  private def fieldsSchema = schema.map(obj => {
    (obj.name, obj.fields)
  }).toMap

  @GET
  @Produces(Array(APPLICATION_JSON))
  def getSchema = {
    val stream = new StreamingOutput {
      import java.io.OutputStream

      override def write(output: OutputStream) {
        mapper.writeValue(output, fieldsSchema)
      }
    }
    Response.ok(stream).build()
  }
}
