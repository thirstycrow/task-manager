package com.github.thirstycrow.taskkeeper

import org.bson.BsonType
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.FindOneAndUpdateOptions
import org.mongodb.scala.model.ReturnDocument
import org.mongodb.scala.model.Updates._

import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Timer
import com.twitter.util.Duration

trait TaskKeeperCollections {

  val db: MongoDatabase

  def schedules = db.getCollection("schedules")
}

trait TaskKeeperOperations {

  self: TaskKeeperCollections =>

  val timer: Timer

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

  def assign(schedule: Schedule[_], timeout: Option[Duration] = None): Future[Option[Assignment]] = {
    val now = Time.now
    val _timeout = timeout.getOrElse(schedule.timeout)
    schedules.findOneAndUpdate(
      filter = and(
        equal("_id", schedule.id),
        equal("status", Assignable.value)),
      update = combine(
        set("status", Assigned.value),
        set("assignment", schedule.assign(_timeout).toBson)),
      options = FindOneAndUpdateOptions()
        .returnDocument(ReturnDocument.AFTER))
      .map(_("assignment").asDocument(): Assignment)
      .toTwitterFuture()
      .map(_.headOption)
      .onSuccess {
        case None =>
        case Some(assignment) =>
          timer.doLater(_timeout) {
            this.timeout(schedule, assignment)
          }
      }
  }

  def timeout(schedule: Schedule[_], assignment: Assignment): Future[Option[Assignment]] = {
    val now = Time.now
    schedules.findOneAndUpdate(
      filter = and(
        equal("_id", schedule.id),
        equal("assignment._id", assignment.id),
        equal("status", Assigned.value),
        lte("assignment.timeout_at", now.toDate)),
      update = combine(
        set("status", Assignable.value)),
      options = FindOneAndUpdateOptions()
        .returnDocument(ReturnDocument.BEFORE))
      .map(_("assignment").asDocument(): Assignment)
      .toTwitterFuture()
      .map(_.headOption)
  }
}

class TaskKeeper(val db: MongoDatabase, val timer: Timer)
  extends TaskKeeperCollections
  with TaskKeeperOperations
