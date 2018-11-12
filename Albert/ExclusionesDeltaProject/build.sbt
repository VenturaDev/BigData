name := "ExclusionesDeltaProject"

version := "0.1"

scalaVersion := "2.10.6"

resolvers ++= Seq(
  "Cloudera repository" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "maven-central" at "http://repo1.maven.org/maven2",
  "conjars.org" at "http://conjars.org/repo"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.14.2" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0-cdh5.14.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3" % "provided"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.14.2" % "provided"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.14.2" % "provided"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.14.2" % "provided"
libraryDependencies += "org.apache.hbase" % "hbase-spark" % "1.2.0-cdh5.14.2"
