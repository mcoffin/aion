package com.netscout.aion2.except

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object AionExceptionHelper {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  
  /**
   * Creates the entity object that should be returned for a given error message
   */
  private[except] def entityForErrorMessage(message: String) = {
    val obj = Map(
      "error" -> message
    )
    mapper writeValueAsString obj
  }
}
