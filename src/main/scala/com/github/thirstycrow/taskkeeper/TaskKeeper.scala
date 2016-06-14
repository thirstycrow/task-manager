package com.github.thirstycrow.taskkeeper

import org.bson.BsonType
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.BsonString
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
import com.twitter.util.NoStacktrace
import com.twitter.util.Promise.K
import org.bson.types.ObjectId
import com.twitter.util.Promise.K

trait TaskKeeperCollections {

  val db: MongoDatabase

  def schedules = db.getCollection("schedules")

  def assignments = db.getCollection("assignments")
}

trait TaskKeeperOperations {

  self: TaskKeeperCollections =>

  val timer: Timer

  def schedule[K](
    task: Task[K],
    when: Time)(
      implicit ev: K => BsonValue): Future[Schedule[K]] = {
    val now = Time.now
    val schedule = Schedule(
      id = ObjectId.get,
      task = task,
      nextTime = when,
      status = Assignable,
      createdAt = now,
      updatedAt = now)
    schedules.insertOne(schedule)
      .toTwitterFuture()
      .map(_ => schedule)
  }

  def findSchedule[K](id: ObjectId)(implicit ev: BsonValue => K): Future[Option[Schedule[K]]] = {
    schedules.find(equal("_id", id))
      .toTwitterFuture()
      .map(_.headOption.map(Schedule(_)))
  }

  def findAssignment(id: ObjectId): Future[Option[Assignment]] = {
    assignments.find(equal("_id", id))
      .toTwitterFuture()
      .map(_.headOption.map(Assignment(_)))
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
        equal("status", Assignable.value),
        lte("next_time", now.toDate)),
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
        set("status", Timeout.value)),
      options = FindOneAndUpdateOptions()
        .returnDocument(ReturnDocument.BEFORE))
      .map(_("assignment").asDocument(): Assignment)
      .toTwitterFuture()
      .map(_.headOption)
      .onSuccess {
        case None =>
        case Some(assignment) =>
          save(assignment)
      }
  }

  def save(assignment: Assignment): Future[Unit] = {
    assignments.insertOne(assignment)
      .toTwitterFuture()
      .unit
  }

  def success[T](schedule: Schedule[T], assignment: Assignment, result: BsonValue = BsonString("OK")): Future[Unit] = {
    val now = Time.now
    schedules.findOneAndUpdate(
      filter = and(
        equal("_id", schedule.id),
        equal("assignment._id", assignment.id)),
      update = combine(
        set("status", Accomplished.value),
        unset("assignment")),
      options = FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE))
      .map(_("assignment").asDocument())
      .map(Assignment.fromBson(_))
      .toTwitterFuture()
      .map(_.headOption)
      .flatMap {
        case Some(assignment) =>
          assignments.insertOne(
            assignment.copy(
              finishedAt = Some(now),
              result = Some(result))
              .toDocument)
            .toTwitterFuture()
            .unit
        case None =>
          throw BadAssignmentException(assignment)
      }
  }
}

class TaskKeeper(val db: MongoDatabase, val timer: Timer)
  extends TaskKeeperCollections
  with TaskKeeperOperations

case class BadAssignmentException(assignment: Assignment) extends RuntimeException with NoStacktrace
