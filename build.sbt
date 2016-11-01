import CommonDependency.dependencies

organization in ThisBuild := "io.github.junheng.akka"

lazy val `akka-locator` = (project in file("."))
  .settings(
    name := "akka-locator",
    version := "0.14-SNAPSHOT",
    scalaVersion := "2.11.7",
    libraryDependencies ++= dependencies.common,
    libraryDependencies ++= dependencies.akka,
    libraryDependencies ++= dependencies.curator,
    libraryDependencies ++= Seq(
      "io.github.junheng.akka" %% "akka-monitor" % "0.2-SNAPSHOT" withSources()
    )
  )
