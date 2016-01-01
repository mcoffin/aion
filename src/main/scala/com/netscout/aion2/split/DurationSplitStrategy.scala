package com.netscout.aion2.split

import com.netscout.aion2.SplitStrategy
import com.netscout.aion2.except.IllegalQueryException
import com.netscout.aion2.model.QueryStrategy
import com.typesafe.config.Config

import java.time.{Duration, Instant}
import java.util.Date

import javax.ws.rs.core.MultivaluedMap

import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration.FiniteDuration

class DurationSplitStrategy(maybeCfg: Option[Config]) extends SplitStrategy {
  val cfg = maybeCfg match {
    case Some(x) => x
    case None => throw new Exception("Configuration must be supplied for a DurationSplitStrategy")
  }

  val duration = {
    val durationStr = cfg.as[String]("duration")
    Duration.parse(durationStr)
  }

  class RangeQueryStrategy (
    val fromDate: Instant,
    val toDate: Instant
  ) extends QueryStrategy {
    import scala.language.implicitConversions

    implicit def instantToDate(i: Instant) = new Date(i.toEpochMilli)

    override def minimum = fromDate
    override def maximum = toDate

    private def durTime = duration.getSeconds

    private def minRow = {
      val minTime = fromDate.getEpochSecond
      val minRowTime = minTime - (minTime % durTime)
      Instant.EPOCH.plusSeconds(minRowTime)
    }

    private def maxRow = {
      val maxTime = toDate.getEpochSecond
      val maxRowTime = maxTime - (maxTime % durTime)
      Instant.EPOCH.plusSeconds(maxRowTime)
    }

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
          val minDate: Date = min
          val maxDate: Date = max
          Some((minDate.asInstanceOf[Object], maxDate.asInstanceOf[Object]))
        }
      }
    }

    override def partialRows = {
      if (minRow.equals(maxRow)) {
        Seq(minRow)
      } else {
        Seq(minRow, maxRow)
      }
    }
  }

  override def strategyForQuery(params: MultivaluedMap[String, String]) = {
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

    if (toDate.isBefore(fromDate)) {
      throw new IllegalQueryException("\'from\' date must be before \'to\' date", null)
    }

    new RangeQueryStrategy(fromDate, toDate)
  }
}
