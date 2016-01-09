package com.netscout.aion2.source

import com.datastax.driver.core._
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.racc.tscg.TypesafeConfig
import com.google.inject.Inject
import com.netscout.aion2.model.{AionObjectConfig, AionIndexConfig, QueryStrategy, DataSource}

class CassandraDataSource @Inject() (
  @TypesafeConfig("com.netscout.aion2.cassandra.keyspace") val keyspaceName: String,
  val mapper: ObjectMapper,
  val session: Session
) extends DataSource {
  import com.datastax.driver.core.querybuilder.QueryBuilder
  import java.util.UUID
  import scala.collection.JavaConversions._

  /**
   * Additional methods for AionObjectConfig used by
   * [[com.netscout.aion2.source.CassandraDataSource]]
   */
  implicit class CassandraAionObject(val obj: AionObjectConfig) {
    /**
     * Gets the title of the row returned in a query for a given field name
     */
    def selectionOfField(field: String) = {
      Option(obj.fields.get(field)) match {
        case Some("timeuuid") => s"system.dateof(${field})"
        case _ => field
      }
    }

    /**
     * Parses any aion objects that were stored differently in cassandra back in to their Aion
     * representations
     *
     * @param key the name of the field we're transforming
     * @param aionObject the Aion value of the field we're transforming
     * @return the Aion representation of the field's value
     */
    def aionResponseForQueryObject(key: String, maybeQueryObject: AnyRef) = {
      val field = for {
        queryObject <- Option(maybeQueryObject)
        fType <- Option(obj.fields.get(key))
      } yield (queryObject, fType)
      field match {
        case Some((queryObject, "json")) => mapper.readTree(queryObject.toString)
        case _ => maybeQueryObject
      }
    }

    /**
     * Transforms aion objects in to the representation that they will be stored in in cassandra
     *
     * @param key the name of the field we're transforming
     * @param aionObject the Aion value of the field we're transforming
     * @return The Cassandra representation of the field's value
     */
    def queryObjectForAionResponse(key: String, aionObject: AnyRef) = Option(obj.fields.get(key)) match {
      case Some("json") => mapper.writeValueAsString(aionObject)
      case _ => aionObject
    }
  }

  /**
   * Transforms an aion type in to the underlying type used in cassandra
   */
  def cassandraTypeForType(aionType: String) = aionType match {
    case "json" => "text"
    case _ => aionType
  }

  /**
   * Gets the name of the split row key for a given column
   *
   * @param columnName the name of the split column
   */
  private def splitRowKey(columnName: String) = s"${columnName}_row"

  /**
   * Gets the type for the row key given a split key type
   *
   * @param splitKeyType the type of the regular split key column
   * @return the name of the type for the split row key column
   */
  private def rowKeyType(splitKeyType: String) = splitKeyType match {
    case "timeuuid" => "timestamp"
    case x => x
  }

  override def initializeSchema(objects: Set[AionObjectConfig]) {
    // This guard is needed because reduce will throw UnsupportedOperationException
    // if there are no objects in the schema
    if (objects.size > 0) {
      objects.map(obj => {
        val fieldDefinitions = obj.fields.map(_ match {
          case (k, v) => s"${k} ${cassandraTypeForType(v)}"
        })
        obj.indices.map(index => {
          val partitionKeyPrefix = if (index.partition.size > 0) {
            ", "
          } else {
            ""
          }
          s"CREATE TABLE IF NOT EXISTS ${keyspaceName}.${columnFamilyName(obj, index)} (${splitRowKey(index.split.column)} ${rowKeyType(obj.fields.get(index.split.column).toString)}, ${fieldDefinitions mkString ", "}, PRIMARY KEY ((${splitRowKey(index.split.column)}${partitionKeyPrefix}${index.partition mkString ", "}), ${index.split.column}))"
        })
      }).reduce(_++_).foreach(session.execute(_))
    }
  }

  override def classOfType(t: String) = {
    import com.datastax.driver.core.DataType
    import com.datastax.driver.core.DataType.Name._
    import com.fasterxml.jackson.databind.JsonNode
    import com.netscout.aion2.except._

    t match {
      case "json" => classOf[JsonNode]
      case _ => {
        try {
          val cqlType = DataType.Name.valueOf(t.toUpperCase)
          cqlType match {
            case ASCII => classOf[String]
            case BIGINT => classOf[java.lang.Long]
            case BLOB => classOf[java.nio.ByteBuffer]
            case BOOLEAN => classOf[Boolean]
            case COUNTER => classOf[Long]
            case DECIMAL => classOf[java.math.BigDecimal]
            case DOUBLE => classOf[Double]
            case FLOAT => classOf[Float]
            case INT => classOf[Int]
            case TIMESTAMP => classOf[java.util.Date]
            case TIMEUUID => classOf[java.util.UUID]
            case DataType.Name.UUID => classOf[java.util.UUID]
            case TEXT => classOf[String]
            case _ => throw new Exception(s"Invalid CQL type ${cqlType}")
          }
        } catch {
          case (e: IllegalArgumentException) => throw new IllegalTypeException(t, e)
        }
      }
    }
  }

  /**
   * Gets a column family name for a given object and index pairing
   */
  private def columnFamilyName(obj: AionObjectConfig, index: AionIndexConfig) = s"${obj.name}_${index.name}"

  override def insertQuery(obj: AionObjectConfig, values: Map[String, AnyRef]) {
    import com.netscout.aion2.except._

    val queries = obj.indices.map(index => {
      val splitStrategy = index.split.strategy.strategy
      val splitKeyValue = splitStrategy.rowKey(values.get(index.split.column) match {
        case Some(v) => v
        case None => throw new IllegalQueryException(s"The split key ${index.split.column} must be provided for index ${index.name}")
      })

      // Add in the row split key information to the values map
      val fullValues = values ++ Map(splitRowKey(index.split.column) -> splitKeyValue)

      // Now transition any aion types that aren't cassandra types to their matching storage type
      val cassandraFullValues = fullValues.map(_ match {
        case (k, v) => (k, obj.queryObjectForAionResponse(k, v))
      })

      // Here we pull the keys / values out of the tuples.
      // Alternatively, we could go though `toMap.keys` and `toMap.values`
      // but since that has to hash there would likely be a performance hit
      val fieldKeys = cassandraFullValues.map(_ match {
        case (k, _) => k
      })
      val fieldValues = cassandraFullValues.map(_ match {
        case (_, v) => v
      })

      val insertStmt = QueryBuilder.insertInto(keyspaceName, columnFamilyName(obj, index))
        .values(fieldKeys.toList, fieldValues.toList)
      insertStmt
    })

    // TODO: atomically batch queries
    queries.foreach(session.execute(_))
  }

  override def executeQuery(obj: AionObjectConfig, index: AionIndexConfig, query: QueryStrategy, partitionKey: Map[String, AnyRef]) = {
    import com.datastax.driver.core.Row
    import com.netscout.aion2.except._

    val partitionClauses = index.partition.map(p => {
      val pValue = partitionKey.get(p) match {
        case Some(v) => v
        case None => throw new IllegalQueryException(s"Partition key parameter ${p} must be present to query against index ${index.name}")
      }
      QueryBuilder.eq(p, pValue)
    })

    val lowRangeSuffix = Option(obj.fields.get(index.split.column)) match {
      case Some("timeuuid") => QueryBuilder.fcall("minTimeuuid", query.minimum)
      case _ => query.minimum
    }
    val highRangeSuffix = Option(obj.fields.get(index.split.column)) match {
      case Some("timeuuid") => QueryBuilder.fcall("maxTimeuuid", query.maximum)
      case _ => query.maximum
    }

    val selectedFields = obj.fields.keys.filter(f => !index.partition.contains(f))

    val partialQueries = query.partialRows.map(rowKey => {
      println(s"Creating partial query for rowKey: ${rowKey}")
      val splitClauses = Seq(
        QueryBuilder.gte(index.split.column, lowRangeSuffix),
        QueryBuilder.lt(index.split.column, highRangeSuffix),
        QueryBuilder.eq(splitRowKey(index.split.column), rowKey)
      )

      var selectWithFields = QueryBuilder.select()
      selectedFields.foreach(f => {
        Option(obj.fields.get(f)) match {
          case Some("timeuuid") => {
            selectWithFields = selectWithFields.fcall("system.dateof", QueryBuilder.column(f))
          }
          case _ => {
            selectWithFields = selectWithFields.column(f)
          }
        }
      })

      var finishedStmt = selectWithFields.from(keyspaceName, columnFamilyName(obj, index))
        .where()
      (partitionClauses ++ splitClauses).foreach(c => {
        finishedStmt = finishedStmt.and(c)
      })

      finishedStmt
    })

    val queries: Iterable[Statement] = query.fullRows match {
      case Some(fullRows) => {
        val middleQueries = fullRows.map(rowKey => {
          println(s"Creating middle query for rowKey: ${rowKey}")
          var stmt = QueryBuilder.select()
          selectedFields.foreach(f => {
            Option(obj.fields.get(f)) match {
              case Some("timeuuid") => {
                stmt = stmt.fcall("system.dateof", QueryBuilder.column(f))
              }
              case _ => {
                stmt = stmt.column(f)
              }
            }
          })
          var finishedStmt = stmt.from(keyspaceName, columnFamilyName(obj, index))
            .where(QueryBuilder.eq(splitRowKey(index.split.column), rowKey))
          for (pc <- partitionClauses) {
            finishedStmt = finishedStmt.and(pc)
          }
          finishedStmt
        })
        partialQueries ++ middleQueries
      }
      case None => partialQueries
    }
    queries.foreach(q => println(q.toString))

    val selectionsReverseIndex = selectedFields.map(f => (obj.selectionOfField(f), f)).toMap

    // TODO: atomically batch queries
    val results = queries.map(session.execute(_)).map(_.all).reduce(_++_)
    results.map(row => {
      val columnsToGrab = row.getColumnDefinitions.map(_.getName)
      columnsToGrab.map(f => {
        (selectionsReverseIndex.get(f).get, row.getObject(f))
      }).map(_ match {
        case (k, v) => (k, obj.aionResponseForQueryObject(k, v))
      })
    })
  }
}
