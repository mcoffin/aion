package com.netscout.aion2.except

class IllegalTypeException (typeName: String, cause: Throwable = null) extends Exception(s"Illegal type name ${typeName} for DataSource", cause)
