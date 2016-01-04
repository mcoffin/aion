package com.netscout.aion2.split

import com.netscout.aion2.SplitStrategy
import com.netscout.aion2.except.IllegalQueryException
import com.netscout.aion2.model.{QueryStrategy, EmptyQueryStrategy}
import com.typesafe.config.Config

import java.time.{Duration, Instant}
import java.util.Date

import javax.ws.rs.core.MultivaluedMap

import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration.FiniteDuration

class DurationSplitStrategy(maybeCfg: Option[Config]) extends SplitStrategy {
  import java.util.UUID
  import scala.language.implicitConversions

  val cfg = maybeCfg match {
    case Some(x) => x
    case None => throw new Exception("Configuration must be supplied for a DurationSplitStrategy")
  }

  val duration = {
    val durationStr = cfg.as[String]("duration")
    Duration.parse(durationStr)
  }

  private def roundInstant(i: Instant) = {
    val durTime = duration.getSeconds

    val s = i.getEpochSecond
    val roundedS = s - (s % durTime)
    Instant.EPOCH.plusSeconds(roundedS)
  }

  implicit def instantToDate(i: Instant) = Date.from(i)
  implicit def dateToInstant(d: Date) = d.toInstant
  implicit def uuidToInstant(uuid: UUID): Instant = {
    import com.datastax.driver.core.utils.UUIDs

    new Date(UUIDs.unixTimestamp(uuid))
  }

  class InstantRange (
    val start: Instant,
    val end: Instant
  ) extends Iterable[Instant] {
    override def iterator = {
      new Iterator[Instant] {
        var current = start
        override def hasNext = current.plus(duration).isAfter(end)
        override def next = {
          current = current.plus(duration)
          current
        }
      }
    }
  }

  class RangeQueryStrategy (
    val fromDate: Instant,
    val toDate: Instant
  ) extends QueryStrategy {

    override def minimum: Date = fromDate
    override def maximum: Date = toDate

    private def durTime = duration.getSeconds

    private def minRow = roundInstant(fromDate)

    private def maxRow = roundInstant(toDate)

    override def fullRows = {
      import java.time.temporal.ChronoUnit._

      if (fromDate.until(toDate, SECONDS) < durTime) {
        None
      } else {
        val min = minRow.plus(duration)
        val max = maxRow.minus(duration)
        if (max.isBefore(min)) {
          None
        } else {
          Some(new InstantRange(min, max).map(i => instantToDate(i)))
        }
      }
    }

    override def partialRows = {
      val minDate: Date = minRow
      val maxDate: Date = maxRow

      if (minRow.equals(maxRow)) {
        Seq(minDate)
      } else {
        Seq(minDate, maxDate)
      }
    }
  }

  override def rowKey(obj: Object) = {
    val inputInstant: Instant = obj match {
      case x: Instant => x
      case x: Date => x
      case uuid: UUID => uuid
      case _ => throw new IllegalQueryException(s"Value of type ${obj.getClass.getName} cannot be used as a value for DurationSplitStrategy")
    }
    val outputDate: Date = roundInstant(inputInstant)
    outputDate
  }

  override def strategyForQuery(params: MultivaluedMap[String, String]): QueryStrategy = {
    var fromDate: Instant = null
    var toDate: Instant = null
    try {
      fromDate = Instant.parse(params.get("from").get(0) match {
        case null => throw new IllegalQueryException("\'from\' parameter must be supplied", null)
        case x => x
      })
      toDate = Instant.parse(params.get("to").get(0) match {
        case null => throw new IllegalQueryException("\'to\' parameter must be supplied", null)
        case x => x
      })
      if (fromDate == null || toDate == null) {
        throw new Exception("Both \'from\' and \'to\' must parse to non-null dates")
      }
    } catch {
      case (e: NullPointerException) => {
        throw new IllegalQueryException("Both \'from\' and \'to\' query parameters must be supplied", null)
      }
      case (e: Exception) => {
        throw new IllegalQueryException("", e)
      }
    }

    // If the dates are equal, we get to terminate early, because no data will be queried
    if (toDate.equals(fromDate)) {
      return EmptyQueryStrategy
    }

    // If the ordering of the dates is messed up, the query is impossible to perform
    if (toDate.isBefore(fromDate)) {
      throw new IllegalQueryException("\'from\' date must be before \'to\' date", null)
    }

    new RangeQueryStrategy(fromDate, toDate)
  }
}
