name := "PARTTEST"
mainClass := Some("dfep.Partition")

scalaVersion := "2.12.10"

version := "1.1"

// // logLevel in run := Level.Warn
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0" 
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.0" 
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.0"

// libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0" % "provided"
// libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.0" % "provided"
// libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0" % "provided"
// libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.0" % "provided"
// scalacOptions += "-deprecation"

// scalacOptions += "-feature"

resolvers += "Akka Repository" at "https://repo.akka.io/releases/"
resolvers +=  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
