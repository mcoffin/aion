package com.netscout.aion2

import com.github.racc.tscg.TypesafeConfigModule
import com.google.inject.{AbstractModule, Guice}
import com.netscout.aion2.inject._

import javax.ws.rs.core.{Application => JAXRSApplication}

import net.codingwell.scalaguice.ScalaModule

import org.glassfish.jersey.server.ResourceConfig
import org.scalatest._

class ApplicationSpec extends FlatSpec with Matchers {
  import com.typesafe.config.ConfigFactory

  val resourceConfig = new ResourceConfig

  class TestResourceConfigModule extends AbstractModule with ScalaModule {
    override def configure {
      bind[ResourceConfig].toInstance(resourceConfig)
    }
  }

  def namedConfig(name: String) = ConfigFactory.parseResources(classOf[ApplicationSpec], name ++ ".json")

  def namedApplication(name: String) = {
    import net.codingwell.scalaguice.InjectorExtensions._

    val injector = Guice.createInjector(
      TypesafeConfigModule.fromConfig(namedConfig(name)),
      ConfigModule,
      JacksonModule,
      new TestResourceConfigModule)

    injector.instance[Application]
  }

  def defaultApplication = namedApplication("defaults")

  implicit class TestApplicationHelper(val app: JAXRSApplication) {
    def resourceCount = app.getClasses.size + app.getSingletons.size
  }

  it should "be initializable with minimal configuration" in {
    val uut = defaultApplication
    uut should not be (null)
  }

  it should "not register any resources with no objects" in {
    val uut = defaultApplication
    resourceConfig.getClasses should not be (null)
    resourceConfig.getSingletons should not be (null)
    resourceConfig.resourceCount should be (0)
  }
}
