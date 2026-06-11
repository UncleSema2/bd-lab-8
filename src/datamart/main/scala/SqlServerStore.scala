package ru.bigdata.datamart

import io.circe.Json

import java.sql.DriverManager

final class SqlServerStore(host: String, port: Int, database: String, user: String, password: String) {
  private def masterUrl = s"jdbc:sqlserver://$host:$port;encrypt=false;trustServerCertificate=true"
  private def dbUrl = s"jdbc:sqlserver://$host:$port;databaseName=$database;encrypt=false;trustServerCertificate=true"

  def ensureDatabase(): Unit = {
    val conn = DriverManager.getConnection(masterUrl, user, password)
    try {
      conn.createStatement().execute(
        s"IF DB_ID('$database') IS NULL CREATE DATABASE [$database]"
      )
    } finally conn.close()
  }

  def saveTrainingResults(request: Json): Unit = {
    ensureDatabase()
    saveRows("food_predictions", list(request, Protocol.Predictions))
    saveRows("food_metrics", list(request, Protocol.Metrics))
  }

  private def list(json: Json, name: String): List[Json] =
    json.hcursor.downField(name).as[List[Json]].getOrElse(Nil)

  private def saveRows(tableName: String, rows: List[Json]): Unit = {
    if (rows.isEmpty) return
    val cols = rows.head.asObject.map(_.keys.toList).getOrElse(return)
    if (cols.isEmpty) return

    val conn = DriverManager.getConnection(dbUrl, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(s"IF OBJECT_ID('$tableName', 'U') IS NOT NULL DROP TABLE [$tableName]")
      val colDefs = cols.map(c => s"[$c] NVARCHAR(255)").mkString(", ")
      stmt.execute(s"CREATE TABLE [$tableName] ($colDefs)")

      val colNames = cols.map(c => s"[$c]").mkString(", ")
      val placeholders = cols.map(_ => "?").mkString(", ")
      val ps = conn.prepareStatement(s"INSERT INTO [$tableName] ($colNames) VALUES ($placeholders)")

      for (row <- rows) {
        val obj = row.asObject.getOrElse(Json.obj().asObject.get)
        cols.zipWithIndex.foreach { case (colName, i) =>
          ps.setString(i + 1, obj(colName).map(jsonToString).getOrElse(""))
        }
        ps.addBatch()
      }
      ps.executeBatch()
    } finally conn.close()
  }

  private def jsonToString(json: Json): String =
    json.fold("", b => b.toString, n => n.toBigDecimal.map(_.toString).getOrElse(""), identity, _ => json.noSpaces, _ => json.noSpaces)
}
