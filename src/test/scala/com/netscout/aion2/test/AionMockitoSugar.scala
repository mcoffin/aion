package com.netscout.aion2.test

import org.mockito.Matchers

import scala.reflect.ClassTag

object AionMockitoSugar {
  def any[A: ClassTag] = {
    val klass = implicitly[ClassTag[A]].runtimeClass
    Matchers.any(klass.asInstanceOf[Class[A]])
  }
}
