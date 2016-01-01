package com.netscout.aion2.split

import java.time.Instant
import java.time.temporal.ChronoUnit._

import javax.ws.rs.core.{MultivaluedHashMap, MultivaluedMap}

import org.scalatest._

class DurationSplitStrategySpec extends FlatSpec with Matchers {
  private def config(durationStr: String) = {
    import com.typesafe.config.ConfigFactory
    import java.util.Properties

    val props = new Properties()
    props.setProperty("duration", durationStr)
    ConfigFactory.parseProperties(props)
  }

  private def query(durationStr: String): MultivaluedMap[String, String] = {
    import java.time.{Duration, Instant}
    import scala.collection.JavaConversions._

    val start = Instant.EPOCH
    val end = start.plus(Duration.parse(durationStr))

    query(start, end)
  }

  private def query(start: Instant, end: Instant): MultivaluedMap[String, String] = {
    val newMap = new MultivaluedHashMap[String, String](2)
    newMap.add("from", start.toString)
    newMap.add("to", end.toString)
    newMap
  }

  "A DurationSplitStrategy" should "return a single query for data within a row" in {
    val uut = new DurationSplitStrategy(Some(config("P7D")))
    val strategy = uut.strategyForQuery(query("P1D"))
    strategy.fullRows should be (None)
    strategy.partialRows.size should be (1)
  }

  it should "return no row range for data that hits only two rows" in {
    val uut = new DurationSplitStrategy(Some(config("P7D")))
    val strategy = uut.strategyForQuery(query(Instant.EPOCH.plus(1, DAYS), Instant.EPOCH.plus(8, DAYS)))
    strategy.fullRows should be (None)
    strategy.partialRows.size should be (2)
  }

  it should "return a fully featured 3-query strategy when data spans 3+ rows" in {
    val uut = new DurationSplitStrategy(Some(config("P7D")))
    val strategy = uut.strategyForQuery(query(Instant.EPOCH.plus(1, DAYS), Instant.EPOCH.plus(14, DAYS)))
    strategy.fullRows should not be (None)
    strategy.partialRows.size should be (2)
  }
}
