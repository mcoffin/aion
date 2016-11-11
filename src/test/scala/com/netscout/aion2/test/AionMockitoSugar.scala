package com.netscout.aion2.test

import org.mockito.ArgumentMatchers

import scala.reflect.ClassTag

object AionMockitoSugar {
  def any[A: ClassTag] = {
    val klass = implicitly[ClassTag[A]].runtimeClass
    ArgumentMatchers.any(klass.asInstanceOf[Class[A]])
  }
}
