
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "finalHudi",
    idePackagePrefix := Some("com.silu.now")
  )
libraryDependencies ++= Seq(
  // Spark dependencies (use Spark 3.5.0)
  "org.apache.spark" %% "spark-core" % "3.5.4",
  "org.apache.spark" %% "spark-sql" % "3.5.4",
  "org.apache.hudi" %% "hudi-spark-bundle" % "0.14.1",// Check if a compatible version exists


  // AWS Java SDK (for future S3 support)
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.533",

  // PostgreSQL driver for YugabyteDB
  "org.postgresql" % "postgresql" % "42.6.0"
)