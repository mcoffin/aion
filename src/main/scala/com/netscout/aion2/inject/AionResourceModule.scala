package com.netscout.aion2.inject

import com.google.inject.{AbstractModule, Key, Provider}
import com.google.inject.spi.{TypeEncounter, TypeListener}

import java.lang.reflect.{Constructor, Field, Method, Parameter, Type}

import net.codingwell.scalaguice.ScalaModule

import org.reflections.Reflections
import org.reflections.scanners.{FieldAnnotationsScanner, MethodAnnotationsScanner, MethodParameterScanner, TypeAnnotationsScanner}
import org.reflections.util.ClasspathHelper
import org.reflections.util.ConfigurationBuilder
import org.reflections.util.FilterBuilder

import scala.collection.JavaConversions._

object AionResourceModule extends AbstractModule with ScalaModule {
  val reflections = {
    val configBuilder = (new ConfigurationBuilder)
      .filterInputsBy(new FilterBuilder().includePackage(""))
      .setUrls(ClasspathHelper.forPackage(""))
      .setScanners(
        new TypeAnnotationsScanner(),
        new MethodParameterScanner(),
        new MethodAnnotationsScanner(),
        new FieldAnnotationsScanner()
      )
    new Reflections(configBuilder)
  }

  override def configure {
    val annotatedConstructorsParams = reflections.getConstructorsWithAnyParamAnnotated(classOf[AionResource]).map(_.getParameters)
    annotatedConstructorsParams.foreach(bindParameters(_))

    val annotatedMethodParams = reflections.getMethodsWithAnyParamAnnotated(classOf[AionResource]).map(_.getParameters)
    annotatedMethodParams.foreach(bindParameters(_))

    val annotatedFields = reflections.getFieldsAnnotatedWith(classOf[AionResource])
    annotatedFields.foreach(bindField(_))
  }

  def bindField(f: Field) {
    val annotation = f.getAnnotation(classOf[AionResource])
    bindValue(f.getType, f.getAnnotatedType.getType, annotation)
  }

  def bindParameters(params: Array[Parameter]) {
    val annotatedParams = params.filter(_.isAnnotationPresent(classOf[AionResource]))
    for (p <- annotatedParams) {
      val annotation = p.getAnnotation(classOf[AionResource])
      bindValue(p.getType, p.getAnnotatedType.getType, annotation)
    }
  }

  def bindValue(paramClass: Class[_], paramType: Type, annotation: AionResource) {
    import com.netscout.aion2.Application
    import java.io.InputStream

    bind[Option[InputStream]].annotatedWith(annotation).toProvider(new Provider[Option[InputStream]] {
      override def get = Option(classOf[Application].getResourceAsStream(annotation.resourcePath))
    })
  }
}
