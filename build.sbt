import Dependencies._

ThisBuild / version := "0.4.0"
ThisBuild / organization := "com.sagainfo"
ThisBuild / scalaVersion := "2.12.7"

lazy val root = (project in file(".")).
  settings(
	assembly / mainClass := Some("agent.AppMain.Main"),  
    name := "xmlAgent",
    
  )

libraryDependencies += scalaTest % Test
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.20.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.19"
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.1.1"
libraryDependencies += "commons-io" % "commons-io" % "2.6"

scalacOptions ++= Seq("-feature", "-deprecation")
