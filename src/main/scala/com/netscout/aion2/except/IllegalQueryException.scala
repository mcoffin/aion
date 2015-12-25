package com.netscout.aion2.except

/**
 * Exception representing what should happen when an un-handlable query comes in
 */
class IllegalQueryException(message: String, cause: Throwable) extends Exception(message, cause) {
}
