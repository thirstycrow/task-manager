import sbt._

object lib {

  object V {
    val bijection = "0.9.2"
    val logback = "1.1.3"
    val mongoScalaDriver = "1.0.1"
    val scalaTest = "2.2.4"
    val scalaTestEmbedMongo = "0.2.2"
    val slf4j = "1.7.12"
    val util = "6.33.0"
  }

  object bijection {
    val util = "com.twitter" %% "bijection-util" % V.bijection
  }

  object logback {
    val classic = "ch.qos.logback" % "logback-classic" % V.logback
  }

  val mongoScalaDriver = "org.mongodb.scala" %% "mongo-scala-driver" % V.mongoScalaDriver

  val scalaTest = "org.scalatest" %% "scalatest" % V.scalaTest

  val scalaTestEmbedMongo = "com.github.simplyscala" %% "scalatest-embedmongo" % V.scalaTestEmbedMongo

  object slf4j {
    val api = "org.slf4j" % "slf4j-api" % V.slf4j
  }

  object util {
    val core = "com.twitter" %% "util-core" % V.util
  }
}
