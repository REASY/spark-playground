name := "spark-playground"
version := "0.2.0-SNAPSHOT"
scalaVersion := "2.12.8"

fork in run := true

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Ywarn-unused-import"
)
libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "ch.qos.logback" % "logback-classic" % "1.1.8",
  "org.scalactic" %% "scalactic" % "3.0.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "commons-validator" % "commons-validator" % "1.5.1",
  "org.rogach" %% "scallop" % "3.0.3",
  "commons-io" % "commons-io" % "2.4",
  "org.apache.hadoop" % "hadoop-common" % "2.7.2",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.2",
  "org.apache.spark" %% "spark-core" % "2.4.2",
  "org.apache.spark" %% "spark-mllib"  % "2.4.2",
  "org.apache.spark" %% "spark-sql"  % "2.4.2"
)

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"