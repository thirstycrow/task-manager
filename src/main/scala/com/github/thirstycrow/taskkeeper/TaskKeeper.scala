package com.github.thirstycrow.taskkeeper

import org.bson.BsonType
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.FindOneAndUpdateOptions
import org.mongodb.scala.model.IndexModel
import org.mongodb.scala.model.Indexes._
import org.mongodb.scala.model.ReturnDocument
import org.mongodb.scala.model.Updates._

import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Timer
import com.twitter.util.Duration
import com.twitter.util.NoStacktrace
import org.bson.types.ObjectId

trait TaskKeeperCollections {

  val db: MongoDatabase

  val schedules = db.getCollection("schedules")

  val assignments = db.getCollection("assignments")
}

trait TaskKeeperOperations {

  self: TaskKeeperCollections =>

  val timer: Timer

  def schedule[K, P](
    task: Task[K, P],
    when: Time,
    period: Option[Duration] = None)(
      implicit ev: K => BsonValue, evp: P => BsonValue): Future[Schedule[K, P]] = {
    val now = Time.now
    val schedule = Schedule(
      id = ObjectId.get,
      task = task,
      nextTime = when,
      period = period,
      status = Assignable,
      createdAt = now,
      updatedAt = now)
    schedules.insertOne(schedule)
      .toTwitterFuture()
      .map(_ => schedule)
  }

  def findSchedule[K, P](id: ObjectId)(implicit ev: BsonValue => K, evp: BsonValue => P): Future[Option[Schedule[K, P]]] = {
    schedules.find(equal("_id", id))
      .toTwitterFuture()
      .map(_.headOption.map(Schedule(_)))
  }

  def findAssignment(id: ObjectId): Future[Option[Assignment]] = {
    assignments.find(equal("_id", id))
      .toTwitterFuture()
      .map(_.headOption.map(Assignment(_)))
  }

  def fetch[K, P](category: String, count: Int)(implicit ev: BsonValue => K, evp: BsonValue => P): Future[Seq[Schedule[K, P]]] = {
    val now = Time.now
    schedules.find(
      filter = and(
        equal("task.category", category),
        equal("status", Assignable.value),
        lte("next_time", now.toDate)))
      .limit(count)
      .toTwitterFuture()
      .map(_.map(doc => Schedule(doc)(ev, evp)))
  }

  def assign(schedule: Schedule[_, _], timeout: Option[Duration] = None): Future[Option[Assignment]] = {
    val now = Time.now
    val _timeout = timeout.getOrElse(schedule.timeout)
    schedules.findOneAndUpdate(
      filter = and(
        equal("_id", schedule.id),
        equal("status", Assignable.value),
        lte("next_time", now.toDate)),
      update = combine(
        set("status", Assigned.value),
        set("assignment", schedule.assign(_timeout).toDocument)),
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

  def timeout(schedule: Schedule[_, _], assignment: Assignment): Future[Option[Assignment]] = {
    val now = Time.now
    val baseUpdate = unset("assignment")
    val update = schedule.period match {
      case None =>
        combine(
          baseUpdate,
          set("status", Timeout.value))
      case Some(period) =>
        combine(
          baseUpdate,
          set("status", Assignable.value),
          set("next_time", (Time.now + period).toDate))
    }
    schedules.findOneAndUpdate(
      filter = and(
        equal("_id", schedule.id),
        equal("assignment._id", assignment.id),
        equal("status", Assigned.value),
        lte("assignment.timeout_at", now.toDate)),
      update = update,
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
    assignments.insertOne(assignment.toDocument)
      .toTwitterFuture()
      .unit
  }

  def success[K, P](schedule: Schedule[K, P], assignment: Assignment, result: BsonValue = BsonString("OK")): Future[Unit] = {
    val now = Time.now
    val baseUpdate = unset("assignment")
    val update = schedule.period match {
      case None =>
        combine(
          baseUpdate,
          set("status", Accomplished.value))
      case Some(period) =>
        combine(
          baseUpdate,
          set("status", Assignable.value),
          set("next_time", (Time.now + period).toDate))
    }
    schedules.findOneAndUpdate(
      filter = and(
        equal("_id", schedule.id),
        equal("assignment._id", assignment.id)),
      update = update,
      options = FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE))
      .map(_("assignment").asDocument())
      .map(Assignment.fromBson(_))
      .toTwitterFuture()
      .map(_.headOption)
      .flatMap {
        case Some(assignment) =>
          save(assignment.copy(
            finishedAt = Some(now),
            result = Some(result),
            timeoutAt = None))
        case None =>
          updateSuccess(assignment, result)
      }
  }

  def failure[K, P](schedule: Schedule[K, P], assignment: Assignment, error: String): Future[Unit] = {
    val now = Time.now
    val baseUpdate = unset("assignment")
    val update = schedule.period match {
      case None =>
        combine(
          baseUpdate,
          set("status", Failed.value))
      case Some(period) =>
        combine(
          baseUpdate,
          set("status", Assignable.value),
          set("next_time", (Time.now + period).toDate))
    }
    schedules.findOneAndUpdate(
      filter = and(
        equal("_id", schedule.id),
        equal("assignment._id", assignment.id)),
      update = update,
      options = FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE))
      .map(_("assignment").asDocument())
      .map(Assignment.fromBson(_))
      .toTwitterFuture()
      .map(_.headOption)
      .flatMap {
        case Some(assignment) =>
          save(assignment.copy(
            finishedAt = Some(now),
            error = Some(error),
            timeoutAt = None))
        case None =>
          updateFailure(assignment, error)
      }
  }

  def updateSuccess(asgn: Assignment, result: BsonValue): Future[Unit] = {
    val now = Time.now
    assignments.updateOne(
      filter = and(
        equal("_id", asgn.id),
        not(exists("finished_at"))),
      update = combine(
        set("finished_at", now.toDate),
        set("result", result)))
      .toTwitterFuture()
      .map(_.headOption.getOrElse(
        throw BadAssignmentException(asgn)))
      .unit
  }

  def updateFailure(asgn: Assignment, error: String): Future[Unit] = {
    val now = Time.now
    assignments.updateOne(
      filter = and(
        equal("_id", asgn.id),
        not(exists("finished_at"))),
      update = combine(
        set("finished_at", now.toDate),
        set("error", error)))
      .toTwitterFuture()
      .map(_.headOption.getOrElse(
        throw BadAssignmentException(asgn)))
      .unit
  }
}

class TaskKeeper(val db: MongoDatabase, val timer: Timer)
  extends TaskKeeperCollections
  with TaskKeeperOperations

case class BadAssignmentException(assignment: Assignment)
  extends RuntimeException with NoStacktrace
