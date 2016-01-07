package com.netscout.aion2.source

import com.datastax.driver.core._
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.racc.tscg.TypesafeConfig
import com.google.inject.Inject
import com.netscout.aion2.model.{AionObjectConfig, AionIndexConfig, QueryStrategy, DataSource}

class CassandraDataSource @Inject() (
  @TypesafeConfig("com.netscout.aion2.cassandra.contactPoints") contactPoints: java.util.List[String],
  @TypesafeConfig("com.netscout.aion2.cassandra.keyspace") val keyspaceName: String,
  val mapper: ObjectMapper
) extends DataSource {
  import com.datastax.driver.core.querybuilder.QueryBuilder
  import java.util.UUID
  import scala.collection.JavaConversions._

  val cluster = Cluster.builder()
    .addContactPoints(contactPoints : _*)
    .build()

  lazy val session = cluster.connect()

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

    def parseToCassandraObject(field: String, value: String): AnyRef = {
      Option(obj.fields.get(field)) match {
        case Some("timeuuid") => UUID.fromString(value)
        case Some("uuid") => UUID.fromString(value)
        case Some("json") => mapper.readTree(value)
        case _ => field
      }
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
   * Additional methods for AionIndexConfig used by
   * [[com.netscout.aion2.source.CassandraDataSource]]
   */
  implicit class CassandraAionIndex(val idx: AionIndexConfig) {
    /**
     * Gets the fully qualified column family name for the index
     */
    def columnFamilyName = s"${keyspaceName}.${idx.name}"
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
          val rangeKeyDefinition = (Seq(index.split.column) ++ index.range) mkString ", "
          s"CREATE TABLE IF NOT EXISTS ${index.columnFamilyName} (${splitRowKey(index.split.column)} ${rowKeyType(obj.fields.get(index.split.column).toString)}, ${fieldDefinitions mkString ", "}, PRIMARY KEY ((${splitRowKey(index.split.column)}${partitionKeyPrefix}${index.partition mkString ", "}), ${rangeKeyDefinition}))"
        })
      }).reduce(_++_).foreach(session.execute(_))
    }
  }

  override def classOfType(t: String) = {
    import com.datastax.driver.core.DataType
    import com.datastax.driver.core.DataType.Name._
    import com.fasterxml.jackson.databind.JsonNode

    t match {
      case "json" => classOf[JsonNode]
      case _ => {
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
      }
    }
  }

  override def insertQuery(obj: AionObjectConfig, index: AionIndexConfig, values: Map[String, AnyRef], splitKeyValue: AnyRef) {
    // Add in the split key information to the values map
    val fullValues = values ++ Map(splitRowKey(index.split.column) -> splitKeyValue)

    // Now transition any aion types that aren't cassandra types to their matching storage type
    val cassandraFullValues = fullValues.map(_ match {
      case (k, v) => {
        (k, cassandraTypeForType(Option(obj.fields.get(k)).get), v)
      }
    }).map(_ match {
      case (k, "json", v) => {
        val jsonSerialized = mapper.writeValueAsString(v)
        (k, jsonSerialized)
      }
      case (k, _, v) => (k, v)
    }).toMap

    val insertStmt = QueryBuilder.insertInto(keyspaceName, index.name)
      .values(cassandraFullValues.keys.toList, cassandraFullValues.values.toList)
    session.execute(insertStmt)
  }

  override def executeQuery(obj: AionObjectConfig, index: AionIndexConfig, query: QueryStrategy, partitionKeyOrig: Map[String, AnyRef], rangeKeysOrig: Map[String, AnyRef]) = {
    import com.datastax.driver.core.Row

    // First, do any parsing we may have to do on the keys before moving on
    def cassandraObjectMap(original: Map[String, AnyRef]): Map[String, AnyRef] = {
      original.map(_ match  {
        case (k, v: String) => (k, obj.parseToCassandraObject(k, v))
        case (k, v) => (k, v)
      }).toMap
    }

    val partitionKey = cassandraObjectMap(partitionKeyOrig)
    val rangeKeys = cassandraObjectMap(rangeKeysOrig)

    val partitionClauses = index.partition.map(p => s"${p} = ?")

    val lowRangeSuffix = Option(obj.fields.get(index.split.column)) match {
      case Some("timeuuid") =>
        "minTimeuuid(?)"
      case _ => "?"
    }

    val highRangeSuffix = Option(obj.fields.get(index.split.column)) match {
      case Some("timeuuid") =>
        "maxTimeuuid(?)"
      case _ => "?"
    }

    val splitClauses = Seq(s"${index.split.column} >= ${lowRangeSuffix}", s"${index.split.column} < ${highRangeSuffix}", s"${splitRowKey(index.split.column)} = ?")
    val rangeKeyArr = rangeKeys.keys.toArray
    val rangeClauses = rangeKeyArr.map(_ ++ " = ?")
    val whereClauses = partitionClauses ++ splitClauses ++ rangeClauses

    val selectedFields = obj.fields.keys.filter(f => !index.partition.contains(f))
    val selectionsReverseIndex = selectedFields.map(f => (obj.selectionOfField(f), f)).toMap
    val fieldSelections = selectionsReverseIndex.keys

    val minMaxStmtSelect = s"SELECT ${fieldSelections mkString ", "} FROM ${index.columnFamilyName}"
    val minMaxStmtStr = minMaxStmtSelect ++ s" WHERE ${whereClauses mkString " AND "}"
    val minMaxStmt = session.prepare(minMaxStmtStr)
    val partitionConstraints = index.partition.map(p => partitionKey.get(p).get)
    val rangeConstraints = rangeKeyArr.map(rangeKeys.get(_).get)
    val partialQueries = query.partialRows.map(rowKey => {
      val variablesToBind = partitionConstraints ++ Seq(query.minimum, query.maximum, rowKey) ++ rangeConstraints
      new BoundStatement(minMaxStmt).bind(variablesToBind : _*)
    })

    // Now for the middle queries
    val queries: Iterable[Statement] = query.fullRows match {
      case Some(fullRows) => {
        val middlePartitionClauses = index.partition.map(p => {
          QueryBuilder.eq(p, partitionKey.get(p).get)
        })
        val middleQueries = fullRows.map(rowKey => {
          var stmt = QueryBuilder.select()
          selectedFields.foreach(f => {
            obj.fields.get(f) match {
              case "timeuuid" => stmt = stmt.fcall("system.dateOf", QueryBuilder.column(f))
              case _ => stmt = stmt.column(f)
            }
          })
          var unrestrictedStmt = stmt.from(keyspaceName, index.name)
            .where(QueryBuilder.eq(splitRowKey(index.split.column), rowKey))
          middlePartitionClauses.foreach(c => {
            unrestrictedStmt = unrestrictedStmt.and(c)
          })
          unrestrictedStmt
        })
        partialQueries ++ middleQueries
      }
      case None => partialQueries
    }

    val results = queries.map(session.execute(_)).map(_.all).reduce(_++_)
    results.map(row => {
      val columnsToGrab = row.getColumnDefinitions.map(_.getName)
      columnsToGrab.map(f => {
        (selectionsReverseIndex.get(f).get, row.getObject(f))
      })
    })
  }
}
