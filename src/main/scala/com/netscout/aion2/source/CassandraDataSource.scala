package com.netscout.aion2.source

import com.datastax.driver.core._
import com.netscout.aion2.model.{AionObjectConfig, AionIndexConfig, QueryStrategy, DataSource}
import com.typesafe.config.{Config, ConfigException}

import net.ceedubs.ficus.Ficus._

class CassandraDataSource(cfg: Option[Config]) extends DataSource {
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._
  import net.ceedubs.ficus.readers.CollectionReaders._
  import scala.collection.JavaConversions._

  val config = cfg match {
    case Some(x) => x
    case None => throw new Exception("Missing dataSource configuration for CassandraDataSource")
  }

  val cluster = Cluster.builder()
    .addContactPoints(config.as[Array[String]]("contactPoints") : _*)
    .build()

  lazy val session = cluster.connect()

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

    val rangeClauses = Seq(s"${index.split.column} >= ${lowRangeSuffix}", s"${index.split.column} < ${highRangeSuffix}", s"${index.split.column}_row = ?")
    val whereClauses = partitionClauses ++ rangeClauses

    val selectedFields = obj.fields.keys.filter(f => !index.partition.contains(f)).map(f => {
      if (obj.fields.get(f).equals(Some("timeuuid"))) {
        s"system.dateOf(${f})"
      } else {
        f
      }
    })

    val minMaxStmtSelect = s"SELECT ${selectedFields mkString ", "} FROM aion.${index.name}"
    val minMaxStmt = new BoundStatement(session.prepare(minMaxStmtSelect ++ s" WHERE ${whereClauses mkString " AND "}"))
    val partitionConstraints = index.partition.map(p => partitionKey.get(p).get)
    val partialQueries = query.partialRows.map(rowKey => minMaxStmt.bind(partitionConstraints ++ Seq(query.minimum, query.maximum, rowKey) : _*))
    val results = partialQueries.map(session.execute(_)).map(_.all).reduce(_++_)
    results.map(row => {
      selectedFields.map(f => (f, row.getObject(f)))
    })
  }
}
