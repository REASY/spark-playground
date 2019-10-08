name := "spark-playground"
version := "0.2.0-SNAPSHOT"
scalaVersion := "2.11.12"

fork in run := true
javaOptions ++= Seq("-Xmx290G", "-XX:+PrintGCDetails", "-XX:+PrintGCDateStamps", "-Xloggc:gc.log")

resolvers += "MMLSpark Repo" at "https://mmlspark.azureedge.net/maven"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

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
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.rogach" %% "scallop" % "3.0.3",
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-mllib"  % "2.4.3",
  "org.apache.spark" %% "spark-sql"  % "2.4.3",
  "com.microsoft.ml.spark" %% "mmlspark" % "0.18.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      // Concatenate everything in the services directory to keep GeoTools happy.
      case ("services" :: _ :: Nil) =>
        MergeStrategy.concat
      case _ =>
        MergeStrategy.discard
    }
  case x => MergeStrategy.first
}