package com.netscout.aion2.source

import com.google.inject.{Guice, Module, AbstractModule}

import net.codingwell.scalaguice.ScalaModule
import net.codingwell.scalaguice.InjectorExtensions._

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.BDDMockito._
import org.scalatest._
import org.scalatest.mock.MockitoSugar

class CassandraDataSourceSpec extends FlatSpec with Matchers with MockitoSugar {
  import com.netscout.aion2.{AionConfig, ApplicationSpec}
  import com.typesafe.config.ConfigFactory

  class MockedCassandraModule extends AbstractModule with ScalaModule {
    import com.datastax.driver.core.Session

    val session = mock[Session]

    override def configure {
      bind[Session].toInstance(session)
    }
  }

  def defaultModules = {
    import com.github.racc.tscg.TypesafeConfigModule
    import com.netscout.aion2.inject._

    Seq(
      TypesafeConfigModule.fromConfig(ApplicationSpec.namedConfig("defaults")),
      JacksonModule,
      Slf4jLoggerModule,
      AionResourceModule
    )
  }

  def defaultFixture =
    new {
      val testModule = new MockedCassandraModule
      val injector = Guice.createInjector((defaultModules :+ testModule): _*)
      val uut = injector.instance[CassandraDataSource]
    }

  "A CassandraDataSource" should "return JsonNode from classOfType for \"json\" type" in {
    import com.fasterxml.jackson.databind.JsonNode

    val f = defaultFixture

    f.uut.classOfType("json") shouldBe classOf[JsonNode]
  }

  it should "create 1 tables per index for schema in initializeSchema()" in {
    val schemaProvider = new AionConfig(classOf[ApplicationSpec].getResourceAsStream("schema-complete.yml"))

    val f = defaultFixture
    f.uut.initializeSchema(schemaProvider.schema)

    // Because there's 3 indices and one keyspace
    verify(f.testModule.session, times(4)).execute(anyString())
  }

  it should "create correct keyspace in initializeSchema" in {
    val f = defaultFixture
    f.uut.initializeSchema(Set())

    verify(f.testModule.session).execute("CREATE KEYSPACE IF NOT EXISTS aion WITH REPLICATION = {\'class\': \'SimpleStrategy\', \'replication_factor\': 1}")
  }

  it should "create correct table for single partition key index" in {
    val schemaProvider = new AionConfig(classOf[ApplicationSpec].getResourceAsStream("schema-complete.yml"))

    val f = defaultFixture
    f.uut.initializeSchema(schemaProvider.schema)

    verify(f.testModule.session).execute("CREATE TABLE IF NOT EXISTS aion.foo_single_partition (time_row timestamp, partition text, range text, time timeuuid, data blob, PRIMARY KEY ((time_row, partition), time))")
  }

  it should "create correct table for double partition key index" in {
    val schemaProvider = new AionConfig(classOf[ApplicationSpec].getResourceAsStream("schema-complete.yml"))

    val f = defaultFixture
    f.uut.initializeSchema(schemaProvider.schema)

    verify(f.testModule.session).execute("CREATE TABLE IF NOT EXISTS aion.foo_double_partition (time_row timestamp, partition text, range text, time timeuuid, data blob, PRIMARY KEY ((time_row, partition, range), time))")
  }

  it should "create correct table for no partition key index" in {
    val schemaProvider = new AionConfig(classOf[ApplicationSpec].getResourceAsStream("schema-complete.yml"))

    val f = defaultFixture
    f.uut.initializeSchema(schemaProvider.schema)

    verify(f.testModule.session).execute("CREATE TABLE IF NOT EXISTS aion.foo_no_partition (time_row timestamp, partition text, range text, time timeuuid, data blob, PRIMARY KEY ((time_row), time))")
  }

  it should "map classes for cassandra types" in {
    val f = defaultFixture

    val typesToTest = Seq(
      "ascii",
      "bigint",
      "blob",
      "boolean",
      "counter",
      "decimal",
      "double",
      "float",
      "int",
      "timestamp",
      "timeuuid",
      "uuid",
      "text"
    )
    for (t <- typesToTest) {
      f.uut.classOfType(t) should not be (null)
    }
  }

  it should "throw IllegalTypeException for an unrecognized type" in {
    import com.netscout.aion2.except._

    val f = defaultFixture

    a [IllegalTypeException] should be thrownBy {
      f.uut.classOfType("definitely not a cql type")
    }
  }

  it should "query for partial and full data executeQuery" in {
    import com.datastax.driver.core.{Row, Statement, ResultSet}
    import com.netscout.aion2.model.QueryStrategy
    import java.time.Instant
    import java.time.temporal.ChronoUnit._
    import java.util.Date
    import org.mockito.ArgumentMatcher
    import scala.collection.JavaConversions._

    val schema = new AionConfig(classOf[ApplicationSpec].getResourceAsStream("schema-complete.yml")).schema
    val obj = schema.head
    val index = obj.indices.filter(idx => idx.name equals "single_partition").head

    val f = defaultFixture

    val queryStrategy = mock[QueryStrategy]
    given(queryStrategy.minimum).willReturn(Date.from(Instant.EPOCH.plus(1, HOURS)), Seq.empty : _*)
    given(queryStrategy.maximum).willReturn(Date.from(Instant.EPOCH.plus(2, HOURS)), Seq.empty : _*)
    given(queryStrategy.partialRows).willReturn(Seq(Date.from(Instant.EPOCH)))
    given(queryStrategy.fullRows).willReturn(Some(Seq(Date.from(Instant.EPOCH.plus(1, DAYS)))))

    val returnedResults = mock[ResultSet]
    when(returnedResults.all).thenReturn(new java.util.LinkedList[Row])

    given(f.testModule.session.execute(anyString())).willReturn(returnedResults)
    given(f.testModule.session.execute(any(classOf[Statement]))).willReturn(returnedResults)
    
    val response = f.uut.executeQuery(obj, index, queryStrategy, Map("partition" -> "somePartition"))

    verify(f.testModule.session).execute(argThat(new ArgumentMatcher[Statement] {
      override def matches(obj: Object) = obj.toString equals s"SELECT range,system.dateof(time),data FROM aion.foo_single_partition WHERE partition='somePartition' AND time>=minTimeuuid(${Instant.EPOCH.plus(1, HOURS).toEpochMilli}) AND time<maxTimeuuid(${Instant.EPOCH.plus(2, HOURS).toEpochMilli}) AND time_row=${Instant.EPOCH.toEpochMilli};"
    }))
    verify(f.testModule.session).execute(argThat(new ArgumentMatcher[Statement] {
      override def matches(obj: Object) = obj.toString equals s"SELECT range,system.dateof(time),data FROM aion.foo_single_partition WHERE time_row=${Instant.EPOCH.plus(1, DAYS).toEpochMilli} AND partition='somePartition';"
    }))
  }

  it should "insert data during insertQuery" in {
    import com.datastax.driver.core.Statement
    import com.datastax.driver.core.utils.UUIDs

    val schema = new AionConfig(classOf[ApplicationSpec].getResourceAsStream("schema-complete.yml")).schema
    val obj = schema.head

    val f = defaultFixture

    f.uut.insertQuery(obj, Map(
      "partition" -> "somePartition",
      "range" -> "someRange",
      "time" -> UUIDs.timeBased(),
      "data" -> ""
    ))

    // should only be one query because it should be a batched query
    verify(f.testModule.session, times(1)).execute(any(classOf[Statement])) // TODO: better statement matching here
  }
}
