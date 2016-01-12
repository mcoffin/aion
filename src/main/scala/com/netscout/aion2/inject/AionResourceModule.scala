package com.netscout.aion2.inject

import com.google.inject.{AbstractModule, Key, Provider, Inject}
import com.google.inject.name.Named
import com.google.inject.spi.{TypeEncounter, TypeListener}
import com.netscout.aion2.Application

import java.io.InputStream
import java.lang.reflect.{Constructor, Field, Method, Parameter, Type}

import net.codingwell.scalaguice.ScalaModule

import org.reflections.Reflections
import org.reflections.scanners.{FieldAnnotationsScanner, MethodAnnotationsScanner, MethodParameterScanner, TypeAnnotationsScanner}
import org.reflections.util.ClasspathHelper
import org.reflections.util.ConfigurationBuilder
import org.reflections.util.FilterBuilder

import scala.collection.JavaConversions._

/**
 * Provider class for getting the schema.yml resource.
 *
 * If there is a system property com.netscout.aion2.schemaFile, it will load
 * the schema file from that file. If there is not such a property, it will
 * attempt to load the schema.yml resource of the Application class.
 */
class SchemaResourceProvider extends Provider[Option[InputStream]] {
  var schemaResource: Option[String] = None

  @Inject(optional=true)
  def setSchemaResource(@Named("com.netscout.aion2.schemaFile") filename: String) {
    schemaResource = Option(filename)
  }

  override def get = {
    import java.io.FileInputStream

    val maybeSchema = for {
      path <- schemaResource
    } yield new FileInputStream(path)

    Option(maybeSchema.getOrElse(classOf[Application].getResourceAsStream("schema.yml")))
  }
}

/**
 * Guice module for binding Aion resources to input streams
 */
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

  val overriddenResources: Map[String, Class[_ <: Provider[Option[InputStream]]]] = Map(
    "schema.yml" -> classOf[SchemaResourceProvider]
  )

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
    overriddenResources.get(annotation.resourcePath) match {
      case Some(provider) => bind[Option[InputStream]].annotatedWith(annotation).toProvider(provider)
      case None => bind[Option[InputStream]].annotatedWith(annotation).toProvider(new Provider[Option[InputStream]] {
        override def get = Option(classOf[Application].getResourceAsStream(annotation.resourcePath))
      })
    }
  }
}
