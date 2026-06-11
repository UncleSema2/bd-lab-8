ThisBuild / scalaVersion := "2.13.16"
ThisBuild / organization := "ru.bigdata"

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "openfoodfacts-datamart",
    Compile / scalaSource := baseDirectory.value / "main" / "scala",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "4.0.1",
      "com.microsoft.sqlserver" % "mssql-jdbc" % "12.8.1.jre11",
      "io.circe" %% "circe-core" % "0.14.9",
      "io.circe" %% "circe-parser" % "0.14.9"
    ),
    Compile / mainClass := Some("ru.bigdata.datamart.DataMartApp")
  )
