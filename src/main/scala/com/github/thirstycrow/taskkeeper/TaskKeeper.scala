package com.github.thirstycrow.taskkeeper

import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.collection.immutable.Document

import com.twitter.util.Future
import com.twitter.util.Time

trait TaskKeeperCollections {

  val db: MongoDatabase

  def schedules = db.getCollection("schedules")
}

trait TaskKeeperOperations {

  self: TaskKeeperCollections =>

  def schedule[T](
    task: Task[T],
    when: Time)(
      implicit ev: T => BsonValue): Future[Unit] = {
    val now = Time.now
    schedules.insertOne(Document(
      "task" -> Task.toBson(task),
      "next_time" -> when.toDate,
      "status" -> Assignable.value,
      "created_at" -> now.toDate,
      "udpated_at" -> now.toDate))
      .toTwitterFuture()
      .unit
  }
}

class TaskKeeper(val db: MongoDatabase)
  extends TaskKeeperCollections
  with TaskKeeperOperations
