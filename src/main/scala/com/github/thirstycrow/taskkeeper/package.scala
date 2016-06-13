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
        status = ScheduleStatus(doc("status").asString.getValue),
        createdAt = doc.get("created_at").asTime.get,
        updatedAt = doc.get("updated_at").asTime orElse doc.get("created_at").asTime get)
    }
    implicit def toBson[K](s: Schedule[K])(implicit ev: K => BsonValue) = {
      BsonDocument(
        "task" -> Task.toBson(s.task),
        "next_time" -> s.nextTime.toDate,
        "status" -> Assignable.value,
        "created_at" -> s.createdAt.toDate,
        "udpated_at" -> s.updatedAt.toDate)
    }
  }

  case class Schedule[K](
      id: ObjectId,
      task: Task[K],
      nextTime: Time,
      status: ScheduleStatus = Assignable,
      createdAt: Time = Time.now,
      updatedAt: Time = Time.now) {
  }
}
