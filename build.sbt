organization := "com.github.thirstycrow"

name := "task-keeper"

scalaVersion := "2.11.7"

lazy val taskKeeper = project
  .in(file("."))
  .settings(
    libraryDependencies ++= Seq(
      lib.bijection.util,
      lib.mongoScalaDriver,
      lib.slf4j.api,
      lib.util.core
    ),
    libraryDependencies ++= Seq(
      lib.logback.classic % "test",
      lib.scalaTest % "test",
      lib.scalaTestEmbedMongo % "test"
    ),
    fork in Test := true,
    coverageEnabled := true
  )
