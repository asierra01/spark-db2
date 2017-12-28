/**
  * (C) Copyright IBM Corp. 2016
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */
name := "spark-db2"

version := "1.0"

// 2.11.8
scalaVersion := "2.11.8"


resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "Sonatype Repository" at "http://oss.sonatype.org/content/repositories/releases"

// Spark dependencies as provided as they are available in spark runtime
val sparkDependency = "2.2.0"
val commonsCSV = "1.5"

libraryDependencies += "org.apache.spark"   %% "spark-core"        % sparkDependency  % "provided"
libraryDependencies += "org.apache.spark"   %% "spark-streaming"   % sparkDependency  % "provided"
libraryDependencies += "org.apache.spark"   %% "spark-sql"         % sparkDependency  % "provided"
libraryDependencies += "org.apache.spark"   %% "spark-repl"        % sparkDependency  % "provided"
libraryDependencies += "org.apache.spark"   %% "spark-hive"        % sparkDependency  % "provided"

libraryDependencies += "org.apache.commons" % "commons-csv"       % commonsCSV

assemblyJarName in assembly := "spark-db2.jar"

unmanagedJars in Compile += file("lib/db2jcc4.jar")

mainClass in (Compile, run) := Some("HiSpeedRead")
