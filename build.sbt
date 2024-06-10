ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "SparkTest"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"
)