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

    def asDocument = bson.filter(_.isDocument())
      .map(_.asDocument())
  }

  implicit class ObservableHelper[T](val obs: Observable[T]) extends AnyVal {
    def toTwitterFuture() = obs.toFuture().as[TwitterFuture[Seq[T]]]
  }

  object Task {
    implicit def toBson[K, P](task: Task[K, P])(
      implicit ev: K => BsonValue, evp: P => BsonValue): BsonValue = {
      BsonDocument(
        "category" -> task.category,
        "key" -> ev(task.key),
        "params" -> task.params.map(evp))
    }
  }

  case class Task[K, P](category: String, key: K, params: Option[P] = None)

  object ScheduleStatus {

    implicit def toBson(status: ScheduleStatus) = BsonString(status.value)
    implicit def fromBson(bson: BsonValue) = apply(bson.asString.getValue)

    def apply(value: String) = {
      value match {
        case Assignable.value => Assignable
        case Assigned.value => Assigned
        case Accomplished.value => Accomplished
        case Failed.value => Failed
        case Timeout.value => Timeout
      }
    }

    def all = Seq(Assignable, Assigned, Accomplished, Failed, Timeout)
  }
  sealed abstract class ScheduleStatus(val value: String) {
    def toBson = BsonString(value)
    override def toString = value
  }
  object Assignable extends ScheduleStatus("ASSIGNABLE")
  object Assigned extends ScheduleStatus("ASSIGNED")
  object Accomplished extends ScheduleStatus("ACCOMPLISHED")
  object Timeout extends ScheduleStatus("TIMEOUT")
  object Failed extends ScheduleStatus("FAILED")

  object Schedule {

    def apply[K, P](doc: BsonDocument)(implicit ev: BsonValue => K, evp: BsonValue => P): Schedule[K, P] = {
      apply(Document(doc))(ev, evp)
    }

    def apply[K, P](doc: Document)(implicit ev: BsonValue => K, evp: BsonValue => P) = {
      new Schedule(
        id = doc("_id").asObjectId().getValue,
        task = Task[K, P](
          category = doc("category").asString.getValue,
          key = ev(doc("key")),
          params = doc.get("params").map(evp)),
        nextTime = doc.get("next_time").asTime.get,
        period = doc.get("period_ms").asDuration,
        timeout = doc.get("timeout_ms").asDuration.getOrElse(DefaultTimeout),
        status = ScheduleStatus(doc("status").asString.getValue),
        createdAt = doc.get("created_at").asTime.get,
        updatedAt = doc.get("updated_at").asTime orElse doc.get("created_at").asTime get,
        assignment = doc.get("assignment").asDocument.map(Assignment.fromBson(_)))
    }

    implicit def toBson[K, P](s: Schedule[K, P])(implicit ev: K => BsonValue, evp: P => BsonValue) = {
      BsonDocument(
        "_id" -> s.id,
        "category" -> s.task.category,
        "key" -> ev(s.task.key),
        "params" -> s.task.params.map(evp),
        "next_time" -> s.nextTime.toDate,
        "timeout_ms" -> s.timeout.inMilliseconds,
        "period_ms" -> s.period.map(_.inMilliseconds),
        "status" -> Assignable.value,
        "created_at" -> s.createdAt.toDate,
        "udpated_at" -> s.updatedAt.toDate,
        "assignment" -> s.assignment.map(_.toDocument))
    }

    implicit def toDocument[K, P](s: Schedule[K, P])(implicit ev: K => BsonValue, evp: P => BsonValue) = {
      Document(toBson(s)(ev, evp)).filter(!_._2.isNull)
    }
  }

  case class Schedule[K, P](
      id: ObjectId,
      task: Task[K, P],
      nextTime: Time,
      period: Option[Duration],
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
        timeoutAt = Some(now + timeout))
    }
  }

  object Assignment {

    def apply(doc: Document): Assignment = {
      new Assignment(
        id = doc("_id").asObjectId().getValue,
        scheduleId = doc("schedule_id").asObjectId().getValue,
        createdAt = doc.get("created_at").asTime.get,
        timeoutAt = doc.get("timeout_at").asTime,
        finishedAt = doc.get("finished_at").asTime,
        result = doc.get("result"),
        error = doc.get("error").map(_.asString.getValue))
    }

    implicit def fromBson(doc: BsonDocument): Assignment = apply(Document(doc))
  }

  case class Assignment(
      id: ObjectId,
      scheduleId: ObjectId,
      createdAt: Time,
      timeoutAt: Option[Time],
      finishedAt: Option[Time] = None,
      result: Option[BsonValue] = None,
      error: Option[String] = None) {

    def toDocument = {
      Document(
        BsonDocument(
          "_id" -> id,
          "schedule_id" -> scheduleId,
          "created_at" -> createdAt.toDate,
          "timeout_at" -> timeoutAt.map(_.toDate),
          "finished_at" -> finishedAt.map(_.toDate),
          "result" -> result,
          "error" -> error))
        .filter(!_._2.isNull)
    }
  }
}
