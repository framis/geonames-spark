val sparkVersion = "2.0.2"

lazy val root = (project in file(".")).
  settings(
    name := "geoname",
    version := "1.0",
    scalaVersion := "2.11.8"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}