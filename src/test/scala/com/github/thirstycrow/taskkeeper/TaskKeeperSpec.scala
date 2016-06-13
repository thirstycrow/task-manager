package com.github.thirstycrow.taskkeeper

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import scala.concurrent.ExecutionContext.Implicits.global
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.bson.BsonValue
import org.mongodb.scala.model.Filters._
import com.twitter.util.MockTimer
import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.util.Await
import com.twitter.bijection.twitter_util.UtilBijections._
import com.twitter.util.Future
import com.twitter.bijection.Conversion.asMethod
import org.mongodb.scala.bson.collection.immutable.Document
import org.scalatest.GivenWhenThen
import org.mongodb.scala.bson.BsonString
import org.scalatest.FeatureSpec

class TaskKeeperSpec extends FeatureSpec with Matchers with GivenWhenThen with EmbeddedMongodb {

  var tk: TaskKeeper = _

  override def beforeAll() {
    super.beforeAll()
    tk = new TaskKeeper(mongoClient.getDatabase("tk_test"))
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

  def zeroOclock = Time.now.floor(1.day)

  def thisMorning = zeroOclock + 6.hours

  def thisEvening = zeroOclock + 18.hour

  feature("schedule a task") {
    val category = "tk.schedule"

    scenario("schedule a one-time task") {

      Given("a task scheduled at a future time")
      val task = Task(category, Texting("10011110000", "Do some shopping tonight"))
      Time.withTimeAt(zeroOclock) { t =>
        Await.result(tk.schedule(task, thisEvening))
      }

      When("it's not the scheduled time yet")
      Time.withTimeAt(thisMorning) { t =>
        Then("the schedule should NOT be fetched")
        Await.result(tk.fetch(category, 1)) shouldBe Nil
      }

      When("it's already the scheduled time")
      Time.withTimeAt(thisEvening) { t =>
        val schedule = Await.result(tk.fetch[Texting](category, 1)).head
        schedule.task shouldBe task
      }
    }
  }
}
