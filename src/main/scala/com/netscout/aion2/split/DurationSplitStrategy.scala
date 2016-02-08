package com.netscout.aion2.split

import com.netscout.aion2.SplitStrategy
import com.netscout.aion2.except.IllegalQueryException
import com.netscout.aion2.model.{QueryStrategy, EmptyQueryStrategy}

import java.time.{Duration, Instant}
import java.util.Date

import javax.ws.rs.core.MultivaluedMap

import scala.concurrent.duration.FiniteDuration

class DurationSplitStrategy(maybeCfg: Option[Map[String, String]]) extends SplitStrategy {
  import java.util.UUID
  import scala.language.implicitConversions

  val maybeDuration = for {
    cfg <- maybeCfg
    durationStr <- cfg.get("duration")
    d <- Some(Duration.parse(durationStr))
  } yield d
  val duration = maybeDuration match {
    case Some(d) => d
    case None => throw new Exception("duration must be supplied as configuration for DurationSplitStrategy")
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
        var current = start.minus(duration)
        override def hasNext = {
          val possibleNext = current.plus(duration)
          possibleNext.isBefore(end) || possibleNext.equals(end)
        }
        override def next = {
          if (hasNext) {
            current = current.plus(duration)
            current
          } else {
            null
          }
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

  /**
   * Convenience method to parse an Instant from a query parameter string
   *
   * Supports "now" strings, or ISO8601 formatted strings
   *
   * @see java.time.Instant.parse
   * @param str the query parameter string to parse
   * @return a java.time.Instant represented by str
   */
  def parseInstant(str: String) = str match {
    case "now" => Instant.now()
    case _ => Instant.parse(str)
  }

  /**
   * Implicit extensions to MultivaluedMap for retreiving query
   * parameters that should have exactly one value
   */
  implicit class MultivaluedMapExtensions[K, V](
    val map: MultivaluedMap[K, V]
  ) {
    /**
     * Gets a parameter that should have only one value
     *
     * @param key the key to look up in the map
     * @return (optionally) the first value in the map for the desired key
     */
    def getSingleValue(key: String) = for {
      values <- Option(map.get(key))
      value <- Option(values.get(0))
    } yield value
  }

  override def strategyForQuery(params: MultivaluedMap[String, String]): QueryStrategy = {
    // Gets a single-valued param from the multivaluedmap of parameters
    def getManditoryParam(paramName: String) = params.getSingleValue(paramName).getOrElse(throw new IllegalQueryException(s"\'${paramName}\' parameter must be supplied"))
    val parseParam = parseInstant _ compose getManditoryParam _

    var fromDate: Instant = null
    var toDate: Instant = null

    try {
      fromDate = parseParam("from")
      toDate = parseParam("to")
      if (fromDate == null || toDate == null) {
        throw new Exception("Both \'from\' and \'to\' must parse to non-null dates")
      }
    } catch {
      case (e: Exception) => {
        throw new IllegalQueryException("", e) // TODO: investigate why this was ever needed
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
