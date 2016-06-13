package com.github.thirstycrow.taskkeeper

import java.net.ServerSocket

import org.mongodb.scala.MongoClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec

import com.github.simplyscala.MongoEmbedDatabase
import com.github.simplyscala.MongodProps
import org.scalatest.FeatureSpec

trait EmbeddedMongodb extends MongoEmbedDatabase with BeforeAndAfterAll {

  self: FeatureSpec =>

  var mongoProps: MongodProps = _
  var mongoClient: MongoClient = _

  override def beforeAll() {
    val port = ephemeralPort()
    mongoProps = mongoStart(port)
    mongoClient = MongoClient("mongodb://localhost:" + port)
  }

  override def afterAll() {
    mongoStop(mongoProps)
  }

  def ephemeralPort() = {
    val socket = new ServerSocket(0)
    socket.setReuseAddress(false)
    val port = socket.getLocalPort
    socket.close()
    port
  }
}
