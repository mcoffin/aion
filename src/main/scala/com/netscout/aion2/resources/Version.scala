package com.netscout.aion2.resources

import com.google.inject.Inject
import com.netscout.aion2.inject.{AionResource, InjectLogger}

import java.io.InputStream
import java.util.Properties

import javax.ws.rs._
import javax.ws.rs.core.MediaType._

import org.slf4j.Logger

@Path("/version")
class VersionResource @Inject() (
  @AionResource(resourcePath = "version.properties") val versionPropertiesFile: Option[InputStream]
) {
  import com.netscout.aion2.Application
  import javax.ws.rs.core.Response.Status._

  @InjectLogger var logger: Logger = null

  lazy val versionProperties = {
    val props = new Properties
    (for {
      stream <- versionPropertiesFile
    } yield props.load(stream)).getOrElse {
      logger.warn("/version endpoint disabled because of inability to find version.properties resource")
    }
    props
  }

  private def getProperty(key: String) = Option(versionProperties.getProperty(key))

  @GET
  @Produces(Array(TEXT_PLAIN))
  def getVersion = getProperty("version") match {
    case Some(version) => version
    case None => throw new WebApplicationException(NOT_FOUND)
  }
}
