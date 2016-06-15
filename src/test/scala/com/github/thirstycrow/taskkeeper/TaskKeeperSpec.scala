package com.github.thirstycrow.taskkeeper

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext.Implicits.global

import org.bson.types.ObjectId
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.scalatest.FeatureSpec
import org.scalatest.FlatSpec
import org.scalatest.GivenWhenThen

import com.twitter.bijection.Conversion.asMethod
import com.twitter.bijection.twitter_util.UtilBijections._
import com.twitter.conversions.time._
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.MockTimer
import com.twitter.util.Time
import com.twitter.util.Duration

class TaskKeeperSpec extends FeatureSpec with GivenWhenThen with EmbeddedMongodb {

  var tk: TaskKeeper = _

  override def beforeAll() {
    super.beforeAll()
    tk = new TaskKeeper(mongoClient.getDatabase("tk_test"), new MockTimer)
  }

  object Texting {
    implicit def toBson(task: Texting) =
      BsonDocument("mobile" -> task.mobile, "text" -> task.text)

    implicit def fromBson(bson: BsonValue) = {
      val doc = Document(bson.asDocument)
      Texting(
        doc("mobile").asString.getValue,
        doc("text").asString.getValue)
    }
  }
  case class Texting(mobile: String, text: String)

  object Credential {
    implicit def toBson(c: Credential) =
      BsonDocument("username" -> c.username, "password" -> c.password)

    implicit def fromBson(bson: BsonValue) = {
      val doc = Document(bson.asDocument)
      Credential(
        doc("username").asString.getValue,
        doc("password").asString.getValue)
    }
  }
  case class Credential(username: String, password: String)

  def zeroOclock = Time.now.floor(1.day)

  def thisMorning = zeroOclock + 6.hours

  def thisEvening = zeroOclock + 18.hour

  feature("schedule a task") {

    scenario("schedule a one-time task") {
      val schd = schedule("feature.schedule", thisEvening)
      assert(findSchedule(schd.id) == Some(schd))
    }

    scenario("schedule a periodic task") {
      val schd = schedule("feature.schedule", thisEvening, Some(1.hour))
      assert(findSchedule(schd.id) == Some(schd))
    }
  }

  feature("fetch a task") {

    scenario("fetch an assignable task") {
      val category = newCategory
      val schd = schedule(category, thisEvening)
      Time.withTimeAt(thisMorning) { timeControl =>
        assert(fetch(category) == None)
      }
    }

    scenario("NOT fetch a task before its scheduled time") {
      val category = newCategory
      val schd = schedule(category, thisEvening)
      Time.withTimeAt(thisMorning) { timeControl =>
        assert(fetch(category) == None)
      }
    }

    scenario("NOT fetch an un-assignable task") {
      def test(status: ScheduleStatus) {
        val category = newCategory
        val schd = schedule(category, thisEvening)
        updateStatus(schd.id, status)
        Time.withTimeAt(thisEvening) { timeControl =>
          assert(fetch(category) == None)
        }
      }
      ScheduleStatus.all.filter(_ != Assignable).map(test)
    }
  }

  feature("assign a task") {

    scenario("assign an assignable task") {
      val category = newCategory
      val schd = schedule(category, thisEvening)
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd)
        assert(assignment.isDefined)
        assert(assignment.get.scheduleId == schd.id)
        assert(assignment.get.createdAt == Time.now)
      }
    }

    scenario("NOT assign a task before its scheduled time") {
      val category = newCategory
      val schd = schedule(category, thisEvening)
      Time.withTimeAt(thisMorning) { timeControl =>
        assert(assign(schd) == None)
      }
    }

    scenario("NOT assign an un-assignable task") {
      def test(status: ScheduleStatus) {
        val category = newCategory
        val schd = schedule(category, thisEvening)
        updateStatus(schd.id, status)
        Time.withTimeAt(thisEvening) { timeControl =>
          assert(assign(schd) == None)
        }
      }
      ScheduleStatus.all.filter(_ != Assignable).map(test)
    }
  }

  feature("timeout an assignment") {

    scenario("timeout an assignment") {
      val category = newCategory
      val schd = schedule(category, thisEvening)
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd).get
        timeControl.advance(schd.timeout)
        tk.timer.asInstanceOf[MockTimer].tick()
        Thread.sleep(50)
        assert(findSchedule(schd.id).map { schd =>
          assert(schd.status == Timeout)
          assert(schd.assignment == None)
        }.nonEmpty)
        assert(findAssignment(assignment.id).get.timeoutAt == Some(Time.now))
      }
    }

    scenario("timeout a periodic assignment") {
      val category = newCategory
      val schd = schedule(category, thisEvening, Some(1.hour))
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd).get
        timeControl.advance(schd.timeout)
        tk.timer.asInstanceOf[MockTimer].tick()
        Thread.sleep(50)
        assert(findSchedule(schd.id).map { schd =>
          assert(schd.status == Assignable)
          assert(schd.assignment == None)
          assert(schd.nextTime == Time.now + schd.period.get)
        }.nonEmpty)
        assert(findAssignment(assignment.id).get.timeoutAt == Some(Time.now))
      }
    }
  }

  feature("report success") {

    scenario("report success") {
      val category = newCategory
      val schd = schedule(category, thisEvening)
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd).get
        Await.result(tk.success(schd, assignment))
        assert(findSchedule(schd.id).map { schd =>
          assert(schd.status == Accomplished)
          assert(schd.assignment == None)
        }.nonEmpty)
        assert(findAssignment(assignment.id).map { asgn =>
          assert(asgn.finishedAt == Some(Time.now))
          assert(asgn.result == Some(BsonString("OK")))
          assert(asgn.timeoutAt == None)
        }.nonEmpty)
      }
    }

    scenario("report success for periodic task") {
      val category = newCategory
      val schd = schedule(category, thisEvening, Some(1.hour))
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd).get
        Await.result(tk.success(schd, assignment))
        assert(findSchedule(schd.id).map { schd =>
          assert(schd.status == Assignable)
          assert(schd.assignment == None)
          assert(schd.nextTime == Time.now + schd.period.get)
        }.nonEmpty)
        assert(findAssignment(assignment.id).map { asgn =>
          assert(asgn.finishedAt == Some(Time.now))
          assert(asgn.result == Some(BsonString("OK")))
          assert(asgn.timeoutAt == None)
        }.nonEmpty)
      }
    }

    scenario("report success when the assignment is already timed out") {
      val category = newCategory
      val schd = schedule(category, thisEvening)
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd).get
        timeControl.advance(schd.timeout)
        tk.timer.asInstanceOf[MockTimer].tick()
        val timeoutAt = Time.now
        timeControl.advance(1.second)
        Await.result(tk.success(schd, assignment))
        assert(findSchedule(schd.id).map { schd =>
          assert(schd.status == Timeout)
          assert(schd.assignment == None)
        }.nonEmpty)
        assert(findAssignment(assignment.id).map { asgn =>
          assert(asgn.timeoutAt == Some(timeoutAt))
          assert(asgn.finishedAt == Some(Time.now))
          assert(asgn.result == Some(BsonString("OK")))
        }.nonEmpty)
      }
    }

    scenario("report success for periodic task when the assignment is already timed out") {
      val category = newCategory
      val schd = schedule(category, thisEvening, Some(1.hour))
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd).get
        timeControl.advance(schd.timeout)
        tk.timer.asInstanceOf[MockTimer].tick()
        val timeoutAt = Time.now
        timeControl.advance(1.second)
        Await.result(tk.success(schd, assignment))
        assert(findSchedule(schd.id).map { schd =>
          assert(schd.status == Assignable)
          assert(schd.assignment == None)
          assert(schd.nextTime == timeoutAt + schd.period.get)
        }.nonEmpty)
        assert(findAssignment(assignment.id).map { asgn =>
          assert(asgn.timeoutAt == Some(timeoutAt))
          assert(asgn.finishedAt == Some(Time.now))
          assert(asgn.result == Some(BsonString("OK")))
        }.nonEmpty)
      }
    }
  }

  feature("report failure") {

    scenario("report failure") {
      val category = newCategory
      val schd = schedule(category, thisEvening)
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd).get
        Await.result(tk.failure(schd, assignment, "error_message"))
        assert(findSchedule(schd.id).map { schd =>
          assert(schd.status == Failed)
          assert(schd.assignment == None)
        }.nonEmpty)
        assert(findAssignment(assignment.id).map { asgn =>
          assert(asgn.finishedAt == Some(Time.now))
          assert(asgn.error == Some("error_message"))
          assert(asgn.timeoutAt == None)
        }.nonEmpty)
      }
    }

    scenario("report failure for periodic task") {
      val category = newCategory
      val schd = schedule(category, thisEvening, Some(1.hour))
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd).get
        Await.result(tk.failure(schd, assignment, "error_message"))
        assert(findSchedule(schd.id).map { schd =>
          assert(schd.status == Assignable)
          assert(schd.assignment == None)
          assert(schd.nextTime == Time.now + schd.period.get)
        }.nonEmpty)
        assert(findAssignment(assignment.id).map { asgn =>
          assert(asgn.finishedAt == Some(Time.now))
          assert(asgn.error == Some("error_message"))
          assert(asgn.timeoutAt == None)
        }.nonEmpty)
      }
    }

    scenario("report failure when the assignment is already timed out") {
      val category = newCategory
      val schd = schedule(category, thisEvening)
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd).get
        timeControl.advance(schd.timeout)
        tk.timer.asInstanceOf[MockTimer].tick()
        val timeoutAt = Time.now
        timeControl.advance(1.second)
        Await.result(tk.failure(schd, assignment, "error_message"))
        assert(findSchedule(schd.id).map { schd =>
          assert(schd.status == Timeout)
          assert(schd.assignment == None)
        }.nonEmpty)
        assert(findAssignment(assignment.id).map { asgn =>
          assert(asgn.timeoutAt == Some(timeoutAt))
          assert(asgn.finishedAt == Some(Time.now))
          assert(asgn.error == Some("error_message"))
        }.nonEmpty)
      }
    }

    scenario("report failure for periodic task when the assignment is already timed out") {
      val category = newCategory
      val schd = schedule(category, thisEvening, Some(1.hour))
      Time.withTimeAt(thisEvening) { timeControl =>
        val assignment = assign(schd).get
        timeControl.advance(schd.timeout)
        tk.timer.asInstanceOf[MockTimer].tick()
        val timeoutAt = Time.now
        timeControl.advance(1.second)
        Await.result(tk.failure(schd, assignment, "error_message"))
        assert(findSchedule(schd.id).map { schd =>
          assert(schd.status == Assignable)
          assert(schd.assignment == None)
          assert(schd.nextTime == timeoutAt + schd.period.get)
        }.nonEmpty)
        assert(findAssignment(assignment.id).map { asgn =>
          assert(asgn.timeoutAt == Some(timeoutAt))
          assert(asgn.finishedAt == Some(Time.now))
          assert(asgn.error == Some("error_message"))
        }.nonEmpty)
      }
    }
  }

  val sequence = new AtomicInteger(0)

  def newCategory = "category_" + sequence.incrementAndGet()

  def schedule(category: String, when: Time, period: Option[Duration] = None) = {
    val task = Task[Texting, Credential](
        category = category,
        key = Texting("10011110000", "Do some shopping tonight"),
        params = Some(Credential("user", "passwd")))
    Await.result(tk.schedule(task, when, period))
  }

  def findSchedule(id: ObjectId) = {
    Await.result(tk.findSchedule[Texting, Credential](id))
  }

  def findAssignment(id: ObjectId) = {
    Await.result(tk.findAssignment(id))
  }

  def fetch(category: String) = {
    Await.result(tk.fetch[Texting, Credential](category, 1).map(_.headOption))
  }

  def assign(schd: Schedule[_, _]) = {
    Await.result(tk.assign(schd))
  }

  def updateStatus(id: ObjectId, status: ScheduleStatus) {
    Await.result(tk.schedules.updateOne(
      filter = equal("_id", id),
      update = set("status", status.value))
      .toTwitterFuture())
  }
}
