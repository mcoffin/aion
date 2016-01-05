package com.netscout.aion2.source

import com.datastax.driver.core._
import com.github.racc.tscg.TypesafeConfig
import com.google.inject.Inject
import com.netscout.aion2.model.{AionObjectConfig, AionIndexConfig, QueryStrategy, DataSource}

class CassandraDataSource @Inject() (
  @TypesafeConfig("com.netscout.aion2.cassandra.contactPoints") contactPoints: java.util.List[String],
  @TypesafeConfig("com.netscout.aion2.cassandra.keyspace") val keyspaceName: String
) extends DataSource {
  import com.datastax.driver.core.querybuilder.QueryBuilder
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
        case Some("timeuuid") => s"system.dateOf(${field})"
        case _ => field
      }
    }
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
          case (k, v) => s"${k} ${v}"
        })
        obj.indices.map(index => {
          val rangeKeyDefinitionPrefix = if (index.range.size > 0) {
            ", "
          } else {
            ""
          }
          val partitionKeyPrefix = if (index.partition.size > 0) {
            ", "
          } else {
            ""
          }
          val rangeKeyDefinition = rangeKeyDefinitionPrefix ++ (index.range mkString ", ")
          s"CREATE TABLE IF NOT EXISTS ${index.columnFamilyName} (${splitRowKey(index.split.column)} ${rowKeyType(obj.fields.get(index.split.column).toString)}, ${fieldDefinitions mkString ", "}, PRIMARY KEY ((${splitRowKey(index.split.column)}${partitionKeyPrefix}${index.partition mkString ", "})${rangeKeyDefinition}))"
        })
      }).reduce(_++_).foreach(session.execute(_))
    }
  }

  override def classOfType(t: String) = {
    import com.datastax.driver.core.DataType
    import com.datastax.driver.core.DataType.Name._

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
      case UUID => classOf[java.util.UUID]
      case TEXT => classOf[String]
      case _ => throw new Exception(s"Invalid CQL type ${cqlType}")
    }
  }

  override def insertQuery(obj: AionObjectConfig, index: AionIndexConfig, values: Map[String, AnyRef], splitKeyValue: AnyRef) {
    // Add in the split key information to the values map
    val fullValues = values ++ Map(splitRowKey(index.split.column) -> splitKeyValue)

    val insertStmt = QueryBuilder.insertInto(keyspaceName, index.name)
      .values(fullValues.keys.toList, fullValues.values.toList)
    session.execute(insertStmt)
  }

  override def executeQuery(obj: AionObjectConfig, index: AionIndexConfig, query: QueryStrategy, partitionKey: Map[String, AnyRef]) = {
    import com.datastax.driver.core.Row

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

    val rangeClauses = Seq(s"${index.split.column} >= ${lowRangeSuffix}", s"${index.split.column} < ${highRangeSuffix}", s"${splitRowKey(index.split.column)} = ?")
    val whereClauses = partitionClauses ++ rangeClauses

    val selectedFields = obj.fields.keys.filter(f => !index.partition.contains(f))
    val fieldSelections = selectedFields.map(obj.selectionOfField(_))

    val minMaxStmtSelect = s"SELECT ${fieldSelections mkString ", "} FROM ${index.columnFamilyName}"
    val minMaxStmtStr = minMaxStmtSelect ++ s" WHERE ${whereClauses mkString " AND "}"
    val minMaxStmt = session.prepare(minMaxStmtStr)
    val partitionConstraints = index.partition.map(p => partitionKey.get(p).get)
    val partialQueries = query.partialRows.map(rowKey => {
      val variablesToBind = partitionConstraints ++ Seq(query.minimum, query.maximum, rowKey)
      new BoundStatement(minMaxStmt).bind(variablesToBind : _*)
    })

    // Now for the middle queries
    val queries: Iterable[Statement] = query.fullRows match {
      case Some(fullRows) => {
        val middlePartitionClauses = index.partition.map(p => {
          QueryBuilder.eq(p, partitionKey.get(p).get)
        })
        val middleQueries = fullRows.map(rowKey => {
          var stmt = QueryBuilder.select(selectedFields.toArray : _*)
            .from(keyspaceName, index.name)
            .where(QueryBuilder.eq(splitRowKey(index.split.column), rowKey))
          middlePartitionClauses.foreach(c => {
            stmt = stmt.and(c)
          })
          stmt
        })
        partialQueries ++ middleQueries
      }
      case None => partialQueries
    }

    val results = queries.map(session.execute(_)).map(_.all).reduce(_++_)
    results.map(row => {
      selectedFields.map(f => (f, row.getObject(obj.selectionOfField(f))))
    })
  }
}
