package com.netscout.aion2.resources

import com.google.inject.Inject
import com.netscout.aion2.inject.InjectLogger

import java.util.Properties

import javax.ws.rs._
import javax.ws.rs.core.MediaType._

import org.slf4j.Logger

@Path("/version")
class VersionResource {
  import com.netscout.aion2.Application
  import javax.ws.rs.core.Response.Status._

  @InjectLogger var logger: Logger = null

  private[resources] val versionProperties = new Properties
  (for {
    stream <- Option(classOf[Application].getResourceAsStream("version.properties"))
  } yield versionProperties.load(stream)).getOrElse {
    logger.warn("/version endpoint disabled because of inability to find version.properties resource")
  }

  private def get(key: String) = Option(versionProperties.getProperty(key))

  @GET
  @Produces(Array(TEXT_PLAIN))
  def getVersion = get("version") match {
    case Some(version) => version
    case None => throw new WebApplicationException(NOT_FOUND)
  }
}
