package com.netscout.aion2.inject

import com.google.inject.AbstractModule
import com.google.inject.TypeLiteral
import com.google.inject.spi.TypeListener
import com.google.inject.spi.TypeEncounter
import com.google.inject.MembersInjector
import com.google.inject.matcher.Matchers

import java.lang.reflect.Field

import net.codingwell.scalaguice.ScalaModule

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object Slf4jLoggerModule extends AbstractModule with ScalaModule {
  class Slf4jMembersInjector[T] (val field: Field) extends MembersInjector[T] {
    val logger = LoggerFactory.getLogger(field.getDeclaringClass)
    field.setAccessible(true)

    override def injectMembers(t: T) = field.set(t, logger)
  }

  class Slf4jTypeListener extends TypeListener {
    class TypeIterator[T](typeLiteral: TypeLiteral[T]) extends Iterator[Class[_ >: T]] {
      var runner = typeLiteral.getRawType

      override def hasNext = runner != classOf[Object]

      override def next() = {
        val cache = runner
        runner = runner.getSuperclass()
        cache
      }
    }

    override def hear[T](typeLiteral: TypeLiteral[T], typeEncounter: TypeEncounter[T]) {
      val iter = new TypeIterator(typeLiteral)
      for (c <- iter) {
        c.getDeclaredFields.filter(field => field.getType.equals(classOf[Logger]) && field.isAnnotationPresent(classOf[InjectLogger])).foreach(field => {
          typeEncounter.register(new Slf4jMembersInjector[T](field))
        })
      }
    }
  }

  override def configure {
    bindListener(Matchers.any(), new Slf4jTypeListener())
  }
}
