package com.netscout.aion2.source

import com.datastax.driver.core._
import com.netscout.aion2.model.{AionObjectConfig, AionIndexConfig, QueryStrategy, DataSource}
import com.typesafe.config.{Config, ConfigException}

import net.ceedubs.ficus.Ficus._

class CassandraDataSource(cfg: Option[Config]) extends DataSource {
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._
  import net.ceedubs.ficus.readers.CollectionReaders._
  import scala.collection.JavaConversions._

  val keyspaceName = "aion"

  val config = cfg match {
    case Some(x) => x
    case None => throw new Exception("Missing dataSource configuration for CassandraDataSource")
  }

  val cluster = Cluster.builder()
    .addContactPoints(config.as[Array[String]]("contactPoints") : _*)
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
      if (obj.fields.get(field).equals(Some("timeuuid"))) {
        s"system.dateOf(${field})"
      } else {
        field
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

  override def insertQuery(obj: AionObjectConfig, index: AionIndexConfig, values: Map[String, AnyRef]) {
    import com.datastax.driver.core.querybuilder.QueryBuilder

    val insertStmt = QueryBuilder.insertInto(keyspaceName, index.name)
      .values(values.keys.toList, values.values.toList)
    session.execute(insertStmt)
  }

  override def executeQuery(obj: AionObjectConfig, index: AionIndexConfig, query: QueryStrategy, partitionKey: Map[String, AnyRef]) = {
    val partitionClauses = index.partition.map(p => s"${p} = ?")

    val lowRangeSuffix = obj.fields.get(index.split.column) match {
      case Some("timeuuid") =>
        "minTimeuuid(?)"
      case _ => "?"
    }

    val highRangeSuffix = obj.fields.get(index.split.column) match {
      case Some("timeuuid") =>
        "maxTimeuuid(?)"
      case _ => "?"
    }

    val rangeClauses = Seq(s"${index.split.column} >= ${lowRangeSuffix}", s"${index.split.column} < ${highRangeSuffix}", s"${splitRowKey(index.split.column)} = ?")
    val whereClauses = partitionClauses ++ rangeClauses

    val selectedFields = obj.fields.keys.filter(f => !index.partition.contains(f))
    val fieldSelections = selectedFields.map(obj.selectionOfField(_))

    val minMaxStmtSelect = s"SELECT ${fieldSelections mkString ", "} FROM ${index.columnFamilyName}"
    val minMaxStmt = new BoundStatement(session.prepare(minMaxStmtSelect ++ s" WHERE ${whereClauses mkString " AND "}"))
    val partitionConstraints = index.partition.map(p => partitionKey.get(p).get)
    val partialQueries = query.partialRows.map(rowKey => minMaxStmt.bind(partitionConstraints ++ Seq(query.minimum, query.maximum, rowKey) : _*))
    val results = partialQueries.map(session.execute(_)).map(_.all).reduce(_++_)
    results.map(row => {
      selectedFields.map(f => (f, row.getObject(obj.selectionOfField(f))))
    })
  }
}
