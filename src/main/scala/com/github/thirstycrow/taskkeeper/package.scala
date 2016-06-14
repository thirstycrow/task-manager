package com.github.thirstycrow

import org.bson.types.ObjectId
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.bson.collection.immutable.Document
import com.twitter.util.Duration
import com.twitter.util.{ Future => TwitterFuture }
import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._
import org.mongodb.scala.Observable
import scala.concurrent.ExecutionContext.Implicits.global

package object taskkeeper {

  val DefaultTimeout = 1.minute

  implicit class BsonHelper(val bson: Option[BsonValue]) extends AnyVal {

    def asDuration = bson.filter(_.isInt64())
      .map(d => Duration.fromMilliseconds(d.asInt64.getValue))

    def asTime = bson.filter(_.isDateTime())
      .map(d => Time.fromMilliseconds(d.asDateTime.getValue))

    def asInt = bson.filter(_.isInt32()).map(_.asInt32.getValue)
  }

  implicit class ObservableHelper[T](val obs: Observable[T]) extends AnyVal {
    def toTwitterFuture() = obs.toFuture().as[TwitterFuture[Seq[T]]]
  }

  object Task {
    implicit def toBson[K](task: Task[K])(implicit ev: K => BsonValue): BsonValue = {
      BsonDocument(
        "category" -> task.category,
        "key" -> ev(task.key))
    }
  }

  case class Task[K](category: String, key: K)

  object ScheduleStatus {

    implicit def toBson(status: ScheduleStatus) = BsonString(status.value)
    implicit def fromBson(bson: BsonValue) = apply(bson.asString.getValue)

    def apply(value: String) = {
      value match {
        case Assignable.value => Assignable
        case Assigned.value => Assigned
        case Accomplished.value => Accomplished
        case Failed.value => Failed
      }
    }
  }
  sealed abstract class ScheduleStatus(val value: String) {
    def toBson = BsonString(value)
    override def toString = value
  }
  object Assignable extends ScheduleStatus("ASSIGNABLE")
  object Assigned extends ScheduleStatus("ASSIGNED")
  object Accomplished extends ScheduleStatus("ACCOMPLISHED")
  object Failed extends ScheduleStatus("FAILED")

  object Schedule {
    def apply[K](doc: Document)(implicit ev: BsonValue => K) = {
      val task = Document(doc("task").asDocument())
      new Schedule(
        id = doc("_id").asObjectId().getValue,
        task = Task[K](
          category = task("category").asString.getValue,
          key = ev(task("key").asDocument)),
        nextTime = doc.get("next_time").asTime.get,
        timeout = doc.get("timeout_ms").asDuration.getOrElse(DefaultTimeout),
        status = ScheduleStatus(doc("status").asString.getValue),
        createdAt = doc.get("created_at").asTime.get,
        updatedAt = doc.get("updated_at").asTime orElse doc.get("created_at").asTime get,
        assignment = doc.get("assignment").map(doc => Assignment.fromBson(doc.asDocument)))
    }
    implicit def toBson[K](s: Schedule[K])(implicit ev: K => BsonValue) = {
      BsonDocument(
        "task" -> Task.toBson(s.task),
        "next_time" -> s.nextTime.toDate,
        "timeout_ms" -> s.timeout.inMilliseconds,
        "status" -> Assignable.value,
        "created_at" -> s.createdAt.toDate,
        "udpated_at" -> s.updatedAt.toDate,
        "assignment" -> s.assignment.map(_.toBson))
    }
  }

  case class Schedule[K](
      id: ObjectId,
      task: Task[K],
      nextTime: Time,
      timeout: Duration = DefaultTimeout,
      status: ScheduleStatus = Assignable,
      createdAt: Time = Time.now,
      updatedAt: Time = Time.now,
      assignment: Option[Assignment] = None) {

    def assign(timeout: Duration) = {
      val now = Time.now
      new Assignment(
        id = ObjectId.get,
        scheduleId = id,
        createdAt = now,
        timeoutAt = now + timeout)
    }
  }

  object Assignment {

    def apply(doc: Document): Assignment = {
      new Assignment(
        id = doc("_id").asObjectId().getValue,
        scheduleId = doc("schedule_id").asObjectId().getValue,
        createdAt = doc.get("created_at").asTime.get,
        timeoutAt = doc.get("timeout_at").asTime.get)
    }

    implicit def fromBson(doc: BsonDocument): Assignment = apply(Document(doc))

    implicit def toBson(a: Assignment) = a.toBson
  }

  case class Assignment(
      id: ObjectId,
      scheduleId: ObjectId,
      createdAt: Time,
      timeoutAt: Time) {

    def toBson = BsonDocument(
      "_id" -> id,
      "schedule_id" -> scheduleId,
      "created_at" -> createdAt.toDate,
      "timeout_at" -> timeoutAt.toDate)
  }
}
