name := "spark-playground"
version := "0.2.0-SNAPSHOT"
scalaVersion := "2.12.15"

fork in run := true
javaOptions ++= Seq("-Xmx18G", "-XX:+PrintGCDetails", "-XX:+PrintGCDateStamps", "-Xloggc:gc.log")

resolvers += "MMLSpark Repo" at "https://mmlspark.azureedge.net/maven"
resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"

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
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "ch.qos.logback" % "logback-classic" % "1.2.11",
  "org.scalatest" %% "scalatest" % "3.2.11" % "test",
  "org.rogach" %% "scallop" % "4.1.0",
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-mllib"  % "3.2.1",
  "org.apache.spark" %% "spark-sql"  % "3.2.1",
  "com.microsoft.azure" %% "synapseml" % "0.9.5"
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
