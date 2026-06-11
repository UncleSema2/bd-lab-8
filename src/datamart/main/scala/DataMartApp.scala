package ru.bigdata.datamart

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import io.circe.Json
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types.NumericType

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}
import java.util.concurrent.{CountDownLatch, Executors}
import scala.util.control.NonFatal
import Protocol._

object DataMartApp {
  private val config = DataMartConfig.load()

  private val spark = SparkSession
    .builder()
    .appName(config.spark.appName)
    .master(config.spark.master)
    .config("spark.driver.memory", config.spark.driverMemory)
    .config("spark.sql.shuffle.partitions", config.spark.shufflePartitions)
    .getOrCreate()

  spark.sparkContext.setLogLevel(config.spark.logLevel)

  private val store = new SqlServerStore(
    config.sqlserver.host,
    config.sqlserver.port,
    config.sqlserver.database,
    config.sqlserver.user,
    config.sqlserver.password
  )

  def main(args: Array[String]): Unit = {
    val server = HttpServer.create(new InetSocketAddress(config.server.host, config.server.port), 0)
    server.setExecutor(Executors.newFixedThreadPool(4))

    route(server, "/v1/training-data") { body =>
      prepareTrainingData(parseBody(body))
    }

    route(server, "/v1/training-results") { body =>
      store.saveTrainingResults(parseBody(body))
      println("Training results saved to SQL Server")
      Json.obj("saved" -> Json.fromBoolean(true))
    }

    sys.addShutdownHook {
      server.stop(0)
      spark.stop()
    }
    server.start()
    println(s"Data mart is listening on ${config.server.host}:${config.server.port}")
    new CountDownLatch(1).await()
  }

  private def prepareTrainingData(request: Json): Json = {
    val inputPath = text(request, InputPath, "data/food.parquet")
    val minNonNullRatio = number(request, MinNonNullRatio, 0.9)
    val targetRows = int(request, TargetRows, 100000)
    val seed = int(request, Seed, 42).toLong

    val raw = spark.read.parquet(resolve(inputPath))
    val totalRows = raw.count()

    if (totalRows == 0) throw new IllegalArgumentException("Input parquet is empty")

    val numericColumns = raw.schema.fields.collect {
      case f if f.dataType.isInstanceOf[NumericType] => f.name
    }.toList

    if (numericColumns.isEmpty) throw new IllegalArgumentException("No numeric columns in input parquet")

    val filledColumns = keepColumnsWithEnoughValues(raw, numericColumns, totalRows, minNonNullRatio)
    val featureColumns = dropConstantColumns(raw, filledColumns)

    if (featureColumns.isEmpty) throw new IllegalArgumentException("No numeric feature columns after preprocessing")

    val prepared = raw
      .select(featureColumns.map(c => F.col(c).cast("double").alias(c)): _*)
      .dropDuplicates()

    val sampled = sampleIfNeeded(prepared, targetRows, seed)
    val rows = sampled.toJSON.collect().flatMap(parseJson)

    Json.obj(
      Rows -> Json.arr(rows: _*),
      FeatureCols -> Json.arr(featureColumns.map(Json.fromString): _*),
      TotalRows -> Json.fromLong(totalRows),
      WorkingRows -> Json.fromInt(rows.length)
    )
  }

  private def keepColumnsWithEnoughValues(
      df: org.apache.spark.sql.DataFrame,
      columns: List[String],
      totalRows: Long,
      minRatio: Double
  ): List[String] = {
    val expressions = columns.map(name => F.count(F.col(name)).alias(name))
    val counts = df.agg(expressions.head, expressions.tail: _*).first()
    val result = columns.filter { name =>
      counts.getAs[Long](name).toDouble / totalRows >= minRatio
    }
    if (result.isEmpty) throw new IllegalArgumentException("No numeric columns with enough non-null values")
    result
  }

  private def dropConstantColumns(
      df: org.apache.spark.sql.DataFrame,
      columns: List[String]
  ): List[String] = {
    val expressions = columns.map(name => F.stddev_samp(F.col(name)).alias(name))
    val stddevs = df.agg(expressions.head, expressions.tail: _*).first()
    columns.filter { name =>
      Option(stddevs.getAs[Double](name)).exists(_ > 0.0)
    }
  }

  private def sampleIfNeeded(
      df: org.apache.spark.sql.DataFrame,
      targetRows: Int,
      seed: Long
  ): org.apache.spark.sql.DataFrame = {
    val rowCount = df.count()
    if (rowCount > targetRows)
      df.sample(withReplacement = false, targetRows.toDouble / rowCount, seed).limit(targetRows)
    else
      df
  }

  private def route(server: HttpServer, path: String)(action: String => Json): Unit = {
    server.createContext(
      path,
      exchange => {
        try {
          if (exchange.getRequestMethod != "POST") {
            write(exchange, 405, error("Method not allowed"))
          } else {
            val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
            write(exchange, 200, ok(action(body)))
          }
        } catch {
          case NonFatal(e) => write(exchange, 400, error(e.getMessage))
        } finally {
          exchange.close()
        }
      }
    )
  }

  private def resolve(path: String): String = {
    val p = Paths.get(path)
    if (p.isAbsolute) p.toString else Path.of(sys.props("user.dir")).resolve(p).normalize().toString
  }

  private def text(json: Json, name: String, default: String): String =
    json.hcursor.downField(name).as[String].getOrElse(default)

  private def int(json: Json, name: String, default: Int): Int =
    json.hcursor.downField(name).as[Int].getOrElse(default)

  private def number(json: Json, name: String, default: Double): Double =
    json.hcursor.downField(name).as[Double].getOrElse(default)

  private def write(exchange: HttpExchange, code: Int, json: Json): Unit = {
    val bytes = json.noSpaces.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.add("Content-Type", "application/json; charset=utf-8")
    exchange.sendResponseHeaders(code, bytes.length)
    exchange.getResponseBody.write(bytes)
  }

  private final case class DataMartConfig(server: ServerConfig, spark: SparkConfig, sqlserver: SqlServerConfig)
  private final case class ServerConfig(host: String, port: Int)
  private final case class SparkConfig(appName: String, master: String, driverMemory: String, shufflePartitions: String, logLevel: String)
  private final case class SqlServerConfig(host: String, port: Int, database: String, user: String, password: String)

  private object DataMartConfig {
    def load(): DataMartConfig = {
      val path = sys.env.getOrElse("DATAMART_CONFIG", "config.json")
      val json = parseBody(scala.io.Source.fromFile(path, "UTF-8").mkString)

      DataMartConfig(
        server = ServerConfig(
          host = env("DATAMART_HOST", textAt(json, "server", "host", "0.0.0.0")),
          port = env("DATAMART_PORT", intAt(json, "server", "port", 8090).toString).toInt
        ),
        spark = SparkConfig(
          appName = textAt(json, "spark", "app_name", "OpenFoodFacts-DataMart"),
          master = textAt(json, "spark", "master", "local[2]"),
          driverMemory = textAt(json, "spark", "driver_memory", "2g"),
          shufflePartitions = textAt(json, "spark", "shuffle_partitions", "4"),
          logLevel = textAt(json, "spark", "log_level", "WARN")
        ),
        sqlserver = SqlServerConfig(
          host = env("MSSQL_HOST", textAt(json, "sqlserver", "host", "mssql")),
          port = textAt(json, "sqlserver", "port", "1433").toInt,
          database = textAt(json, "sqlserver", "database", "food"),
          user = textAt(json, "sqlserver", "user", "sa"),
          password = env("MSSQL_PASSWORD", textAt(json, "sqlserver", "password", "QwErTy123@!"))
        )
      )
    }

    private def env(name: String, default: String): String = sys.env.getOrElse(name, default)

    private def textAt(json: Json, section: String, name: String, default: String): String =
      json.hcursor.downField(section).downField(name).as[String].getOrElse(default)

    private def intAt(json: Json, section: String, name: String, default: Int): Int =
      json.hcursor.downField(section).downField(name).as[Int].getOrElse(default)
  }
}
