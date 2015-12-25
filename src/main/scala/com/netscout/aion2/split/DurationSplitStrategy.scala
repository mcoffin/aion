package com.netscout.aion2.split

import com.netscout.aion2.SplitStrategy
import com.netscout.aion2.except.IllegalQueryException
import com.netscout.aion2.model.QueryStrategy
import com.typesafe.config.Config

import java.time.Duration
import java.util.Date

import javax.ws.rs.core.MultivaluedMap

import net.ceedubs.ficus.Ficus._

import org.joda.time.DateTime

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
    val fromDate: DateTime,
    val toDate: DateTime
  ) extends QueryStrategy {
    override def minimum = fromDate.toDate
    override def maximum = toDate.toDate

    private def durTime = duration.getSeconds

    private def minRow = {
      val minTime = fromDate.toDate.getTime / 1000
      new Date((minTime - (minTime % durTime)) * 1000)
    }

    private def maxRow = {
      val maxTime = toDate.toDate.getTime / 1000
      new Date((maxTime - (maxTime % durTime)) * 1000)
    }

    override def fullRows = {
      val maxTime = toDate.toDate.getTime / 1000
      val minTime = fromDate.toDate.getTime / 1000

      if (maxTime - minTime < durTime) {
        None
      } else {
        val min = new Date((minRow.getTime / 1000) + durTime)
        val max = new Date((maxRow.getTime / 1000) - durTime)
        Some((minTime.asInstanceOf[Object], maxTime.asInstanceOf[Object]))
      }
    }

    override def partialRows = Seq(minRow, maxRow)
  }

  override def strategyForQuery(params: MultivaluedMap[String, String]) = {
    var fromDate: DateTime = null
    var toDate: DateTime = null
    try {
      fromDate = DateTime.parse(params.get("from").get(0) match {
        case null => throw new IllegalQueryException("\'from\' parameter must be supplied", null)
        case x => x
      })
      toDate = DateTime.parse(params.get("to").get(0) match {
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

    new RangeQueryStrategy(fromDate, toDate)
  }
}
