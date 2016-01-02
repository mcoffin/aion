package com.netscout.aion2.split

import com.netscout.aion2.except.IllegalQueryException

import java.time.Instant
import java.time.temporal.ChronoUnit._
import java.util.Date

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

  it should "throw IllegalQueryException when parameters are missing" in {
    val uut = new DurationSplitStrategy(Some(config("P30D")))
    a [IllegalQueryException] should be thrownBy {
      uut.strategyForQuery(new MultivaluedHashMap[String, String]())
    }
  }

  it should "throw IllegalQueryException when parameters aren't dates" in {
    val uut = new DurationSplitStrategy(Some(config("P30D")))
    val paramMap = new MultivaluedHashMap[String, String](2)
    paramMap.add("from", "foo")
    paramMap.add("to", "bar")
    a [IllegalQueryException] should be thrownBy {
      uut.strategyForQuery(paramMap)
    }
  }

  it should "throw IllegalQueryException when 'from' is after 'to'" in {
    val uut = new DurationSplitStrategy(Some(config("P30D")))
    a [IllegalQueryException] should be thrownBy {
      uut.strategyForQuery(query(Instant.EPOCH.plus(1, DAYS), Instant.EPOCH))
    }
  }

  it should "return an empty query for an empty range" in {
    val uut = new DurationSplitStrategy(Some(config("P7D")))
    val strategy = uut.strategyForQuery(query(Instant.EPOCH, Instant.EPOCH))
    strategy.fullRows shouldBe None
    strategy.partialRows.size should be (0)
  }

  it should "round the start time down to row time" in {
    val uut = new DurationSplitStrategy(Some(config("P7D")))
    val realStart = Instant.EPOCH.plus(1, DAYS)
    val strategy = uut.strategyForQuery(query(realStart, realStart.plus(2, DAYS)))
    strategy.partialRows.head should equal (new Date(0))
  }

  it should "round the end time down to row time" in {
    val uut = new DurationSplitStrategy(Some(config("P7D")))
    val realStart = Instant.EPOCH.plus(1, DAYS)
    val strategy = uut.strategyForQuery(query(realStart, realStart.plus(7, DAYS)))
    val rowEnd = Instant.EPOCH.plus(7, DAYS)
    strategy.partialRows.last should equal (new Date(rowEnd.toEpochMilli))
  }

  it should "preserve the originally requested range in 'minimum' and 'maximum'" in {
    val uut = new DurationSplitStrategy(Some(config("P7D")))
    val realStart = Instant.EPOCH.plus(1, DAYS)
    val realEnd = realStart.plus(14, DAYS)
    val strategy = uut.strategyForQuery(query(realStart, realEnd))
    strategy.partialRows.head should equal (new Date(0))
    val fullRowDate = new Date(Instant.EPOCH.plus(7, DAYS).toEpochMilli)
    strategy.fullRows should equal (Some((fullRowDate, fullRowDate)))
    strategy.minimum shouldEqual (new Date(realStart.toEpochMilli))
    strategy.maximum shouldEqual (new Date(realEnd.toEpochMilli))
  }

  it should "return a range of fullRows when data spans 4+ rows" in {
    val uut = new DurationSplitStrategy(Some(config("P7D")))
    val realStart = Instant.EPOCH.plus(1, DAYS)
    val realEnd = realStart.plus(7*4, DAYS)
    val strategy = uut.strategyForQuery(query(realStart, realEnd))

    val firstFullRow = new Date(Instant.EPOCH.plus(7, DAYS).toEpochMilli)
    val lastFullRow = new Date(Instant.EPOCH.plus(3*7, DAYS).toEpochMilli)

    strategy.partialRows.size shouldBe 2
    strategy.fullRows shouldEqual Some((firstFullRow, lastFullRow))
  }
}