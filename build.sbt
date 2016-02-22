lazy val root = (project in file(".")).
  settings(
    name := "bigcoresets",
    version := "1.0",
    scalaVersion := "2.10.6",
    mainClass in Compile := Some("streaming.coresets.App")
  )

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.0" % "provided",
  
  "com.github.scopt" %% "scopt" % "3.3.0",
  "com.jsuereth" %% "scala-arm" % "1.4",
  
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",


  "org.reflections" % "reflections" % "0.9.1",
  
  "univ.ml" % "coreset" % "1.0-SNAPSHOT",

  "junit" % "junit" % "4.8.1" % "test"
)

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}
