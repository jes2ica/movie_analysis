name := "movie_analysis"

version := "0.1"

scalaVersion := "2.11.12"
val liftVersion = "2.4" // Put the current/latest lift version here

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.3.3",
  "io.circe" % "circe-core_2.11" % "0.8.0",
  "io.circe" %% "circe-parser" % "0.8.0",
  "io.circe" %% "circe-generic" % "0.8.0"
)