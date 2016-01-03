package com.netscout.aion2.except

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response
import javax.ws.rs.core.Response.Status._

/**
 * Exception representing what should happen when an un-handlable query comes in
 */
class IllegalQueryException(message: String, cause: Throwable = null) extends WebApplicationException(cause, Response.status(BAD_REQUEST).entity(AionExceptionHelper.entityForErrorMessage(message)).build()) {
}
