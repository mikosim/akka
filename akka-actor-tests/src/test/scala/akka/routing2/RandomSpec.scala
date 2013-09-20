/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing2

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.{ Props, Actor }
import akka.testkit.{ TestLatch, ImplicitSender, DefaultTimeout, AkkaSpec }
import akka.pattern.ask

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class RandomSpec extends AkkaSpec with DefaultTimeout with ImplicitSender {

  "random pool" must {

    "be able to shut down its instance" in {
      val stopLatch = new TestLatch(7)

      val actor = system.actorOf(Props(new Actor {
        def receive = {
          case "hello" ⇒ sender ! "world"
        }

        override def postStop() {
          stopLatch.countDown()
        }
      }).withRouter(RandomPool(7)), "random-shutdown")

      actor ! "hello"
      actor ! "hello"
      actor ! "hello"
      actor ! "hello"
      actor ! "hello"

      within(2 seconds) {
        for (i ← 1 to 5) expectMsg("world")
      }

      system.stop(actor)
      Await.ready(stopLatch, 5 seconds)
    }

    "deliver messages in a random fashion" in {
      val connectionCount = 10
      val iterationCount = 100
      val doneLatch = new TestLatch(connectionCount)

      val counter = new AtomicInteger
      var replies = Map.empty[Int, Int]
      for (i ← 0 until connectionCount) {
        replies = replies + (i -> 0)
      }

      val actor = system.actorOf(Props(new Actor {
        lazy val id = counter.getAndIncrement()
        def receive = {
          case "hit" ⇒ sender ! id
          case "end" ⇒ doneLatch.countDown()
        }
      }).withRouter(RandomPool(connectionCount)), "random")

      for (i ← 0 until iterationCount) {
        for (k ← 0 until connectionCount) {
          val id = Await.result((actor ? "hit").mapTo[Int], timeout.duration)
          replies = replies + (id -> (replies(id) + 1))
        }
      }

      counter.get must be(connectionCount)

      actor ! akka.routing.Broadcast("end")
      Await.ready(doneLatch, 5 seconds)

      replies.values foreach { _ must be > (0) }
      replies.values.sum must be === iterationCount * connectionCount
    }

    "deliver a broadcast message using the !" in {
      val helloLatch = new TestLatch(6)
      val stopLatch = new TestLatch(6)

      val actor = system.actorOf(Props(new Actor {
        def receive = {
          case "hello" ⇒ helloLatch.countDown()
        }

        override def postStop() {
          stopLatch.countDown()
        }
      }).withRouter(RandomPool(6)), "random-broadcast")

      actor ! akka.routing.Broadcast("hello")
      Await.ready(helloLatch, 5 seconds)

      system.stop(actor)
      Await.ready(stopLatch, 5 seconds)
    }
  }
}
