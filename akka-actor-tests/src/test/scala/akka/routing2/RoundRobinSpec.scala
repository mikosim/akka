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
class RoundRobinSpec extends AkkaSpec with DefaultTimeout with ImplicitSender {

  "round robin pool" must {

    "be able to shut down its instance" in {
      val helloLatch = new TestLatch(5)
      val stopLatch = new TestLatch(5)

      val actor = system.actorOf(Props(new Actor {
        def receive = {
          case "hello" ⇒ helloLatch.countDown()
        }

        override def postStop() {
          stopLatch.countDown()
        }
      }).withRouter(RoundRobinPool(5)), "round-robin-shutdown")

      actor ! "hello"
      actor ! "hello"
      actor ! "hello"
      actor ! "hello"
      actor ! "hello"
      Await.ready(helloLatch, 5 seconds)

      system.stop(actor)
      Await.ready(stopLatch, 5 seconds)
    }

    "deliver messages in a round robin fashion" in {
      val connectionCount = 10
      val iterationCount = 10
      val doneLatch = new TestLatch(connectionCount)

      val counter = new AtomicInteger
      var replies = Map.empty[Int, Int]
      for (i ← 0 until connectionCount) {
        replies += i -> 0
      }

      val actor = system.actorOf(Props(new Actor {
        lazy val id = counter.getAndIncrement()
        def receive = {
          case "hit" ⇒ sender ! id
          case "end" ⇒ doneLatch.countDown()
        }
      }).withRouter(RoundRobinPool(connectionCount)), "round-robin")

      for (i ← 0 until iterationCount) {
        for (k ← 0 until connectionCount) {
          val id = Await.result((actor ? "hit").mapTo[Int], timeout.duration)
          replies = replies + (id -> (replies(id) + 1))
        }
      }

      counter.get must be(connectionCount)

      actor ! akka.routing.Broadcast("end")
      Await.ready(doneLatch, 5 seconds)

      replies.values foreach { _ must be(iterationCount) }
    }

    "deliver a broadcast message using the !" in {
      val helloLatch = new TestLatch(5)
      val stopLatch = new TestLatch(5)

      val actor = system.actorOf(Props(new Actor {
        def receive = {
          case "hello" ⇒ helloLatch.countDown()
        }

        override def postStop() {
          stopLatch.countDown()
        }
      }).withRouter(RoundRobinPool(5)), "round-robin-broadcast")

      actor ! akka.routing.Broadcast("hello")
      Await.ready(helloLatch, 5 seconds)

      system.stop(actor)
      Await.ready(stopLatch, 5 seconds)
    }
  }

  "round robin nozzle" must {

    "deliver messages in a round robin fashion" in {
      val connectionCount = 10
      val iterationCount = 10
      val doneLatch = new TestLatch(connectionCount)

      var replies = Map.empty[String, Int].withDefaultValue(0)

      val paths = (1 to 10) map { n ⇒
        val ref = system.actorOf(Props(new Actor {
          def receive = {
            case "hit" ⇒ sender ! self.path.name
            case "end" ⇒ doneLatch.countDown()
          }
        }), name = "target-" + n)
        ref.path.elements.mkString("/", "/", "")
      }

      val actor = system.actorOf(Props.empty.withRouter(RoundRobinNozzle(paths)), "round-robin-nozzle1")

      for (i ← 0 until iterationCount) {
        for (k ← 0 until connectionCount) {
          val id = Await.result((actor ? "hit").mapTo[String], timeout.duration)
          replies = replies + (id -> (replies(id) + 1))
        }
      }

      actor ! akka.routing.Broadcast("end")
      Await.ready(doneLatch, 5 seconds)

      replies.values foreach { _ must be(iterationCount) }
    }
  }

}
