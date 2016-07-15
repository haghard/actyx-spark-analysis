import sbt._

name := "actyx-spark"

organization := "actyx"

version := "0.0.1"

licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

scalaVersion := "2.11.8"
val sparkVersion = "1.6.1"

shellPrompt := { state => System.getProperty("user.name") + "> " }

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

javaOptions ++= Seq("-Xmx2G")

scalacOptions ++= Seq("-deprecation", "-unchecked", "-Xexperimental")

resolvers ++= Seq(
  Resolver.defaultLocal,
  Resolver.mavenLocal,
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases"  at "https://oss.sonatype.org/content/repositories/releases",
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/"
)

val sparkDependencyScope = "provided"

libraryDependencies ++= Seq(
  "io.spray"                %% "spray-json"                % "1.3.2",
  "org.apache.spark"        %% "spark-core"                % sparkVersion      % sparkDependencyScope,
  "org.apache.spark"        %% "spark-sql"                 % sparkVersion      % sparkDependencyScope,
  "org.apache.spark"        %% "spark-streaming"           % sparkVersion      % sparkDependencyScope,
  "org.apache.spark"        %% "spark-streaming-kafka"     % sparkVersion      % sparkDependencyScope,
  "com.datastax.spark"      %% "spark-cassandra-connector" % "1.6.0"           % sparkDependencyScope
)

autoAPIMappings := true

mainClass in Compile := Some("actyx.MovingAverage")

assemblyJarName in assembly := "moving-average.jar"