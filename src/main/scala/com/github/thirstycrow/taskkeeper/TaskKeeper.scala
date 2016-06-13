package com.github.thirstycrow.taskkeeper

import org.bson.BsonType
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Filters._

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

  def fetch[K](category: String, count: Int)(implicit ev: BsonValue => K): Future[Seq[Schedule[K]]] = {
    val now = Time.now
    schedules.find(
      filter = and(
        equal("task.category", category),
        equal("status", Assignable.value),
        lte("next_time", now.toDate)))
      .limit(count)
      .toTwitterFuture()
      .map(_.map(doc => Schedule(doc)(ev)))
  }
}

class TaskKeeper(val db: MongoDatabase)
  extends TaskKeeperCollections
  with TaskKeeperOperations
