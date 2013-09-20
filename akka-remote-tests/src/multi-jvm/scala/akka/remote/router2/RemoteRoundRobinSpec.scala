/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.remote.router2

import language.postfixOps
import scala.collection.immutable
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.PoisonPill
import akka.actor.Address
import scala.concurrent.Await
import akka.pattern.ask
import akka.remote.testkit.{ STMultiNodeSpec, MultiNodeConfig, MultiNodeSpec }
import akka.routing.Broadcast
import akka.routing2.CurrentRoutees
import akka.routing2.RouterRoutees
import akka.routing2.RoundRobinPool
import akka.routing2.RoundRobinNozzle
import akka.routing2.RoutedActorRef
import akka.routing2.Resizer
import akka.testkit._
import scala.concurrent.duration._
import akka.routing2.Routee
import akka.routing.FromConfig

object RemoteRoundRobinMultiJvmSpec extends MultiNodeConfig {

  class SomeActor extends Actor {
    def receive = {
      case "hit" ⇒ sender ! self
    }
  }

  class TestResizer extends Resizer {
    override def isTimeForResize(messageCounter: Long): Boolean = messageCounter <= 10
    override def resize(currentRoutees: immutable.IndexedSeq[Routee]): Int = 1
  }

  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(debugConfig(on = false))

  deployOnAll("""
      /service-hello {
        router = round-robin
        nr-of-instances = 3
        target.nodes = ["@first@", "@second@", "@third@"]
        routing2 = on # FIXME #3549 temporary
      }

      /service-hello2 {
        router = round-robin
        target.nodes = ["@first@", "@second@", "@third@"]
        routing2 = on # FIXME #3549 temporary
      }
      
      /service-hello3 {
        router = round-robin
        routees.paths = [
          "@first@/user/target-first",
          "@second@/user/target-second",
          "@third@/user/target-third"]
        routing2 = on # FIXME #3549 temporary
      }
    """)
}

class RemoteRoundRobinMultiJvmNode1 extends RemoteRoundRobinSpec
class RemoteRoundRobinMultiJvmNode2 extends RemoteRoundRobinSpec
class RemoteRoundRobinMultiJvmNode3 extends RemoteRoundRobinSpec
class RemoteRoundRobinMultiJvmNode4 extends RemoteRoundRobinSpec

class RemoteRoundRobinSpec extends MultiNodeSpec(RemoteRoundRobinMultiJvmSpec)
  with STMultiNodeSpec with ImplicitSender with DefaultTimeout {
  import RemoteRoundRobinMultiJvmSpec._

  def initialParticipants = 4

  "A remote round robin pool" must {
    "be locally instantiated on a remote node and be able to communicate through its RemoteActorRef" taggedAs LongRunningTest in {

      runOn(first, second, third) {
        enterBarrier("start", "broadcast-end", "end")
      }

      runOn(fourth) {
        enterBarrier("start")
        val actor = system.actorOf(Props[SomeActor].withRouter(RoundRobinPool(nrOfInstances = 0)), "service-hello")
        actor.isInstanceOf[RoutedActorRef] must be(true)

        val connectionCount = 3
        val iterationCount = 10

        for (i ← 0 until iterationCount; k ← 0 until connectionCount) {
          actor ! "hit"
        }

        val replies: Map[Address, Int] = (receiveWhile(5 seconds, messages = connectionCount * iterationCount) {
          case ref: ActorRef ⇒ ref.path.address
        }).foldLeft(Map(node(first).address -> 0, node(second).address -> 0, node(third).address -> 0)) {
          case (replyMap, address) ⇒ replyMap + (address -> (replyMap(address) + 1))
        }

        enterBarrier("broadcast-end")
        actor ! Broadcast(PoisonPill)

        enterBarrier("end")
        replies.values foreach { _ must be(iterationCount) }
        replies.get(node(fourth).address) must be(None)

        // shut down the actor before we let the other node(s) shut down so we don't try to send
        // "Terminate" to a shut down node
        system.stop(actor)
      }

      enterBarrier("done")
    }
  }

  "A remote round robin pool with resizer" must {
    "be locally instantiated on a remote node after several resize rounds" taggedAs LongRunningTest in within(5 seconds) {

      runOn(first, second, third) {
        enterBarrier("start", "broadcast-end", "end")
      }

      runOn(fourth) {
        enterBarrier("start")
        val actor = system.actorOf(Props[SomeActor].withRouter(RoundRobinPool(
          nrOfInstances = 1,
          resizer2 = Some(new TestResizer))), "service-hello2")
        actor.isInstanceOf[RoutedActorRef] must be(true)

        actor ! CurrentRoutees
        // initial nrOfInstances 1 + inital resize => 2
        expectMsgType[RouterRoutees].routees.size must be(2)

        val repliesFrom: Set[ActorRef] =
          (for (n ← 3 to 9) yield {
            // each message trigger a resize, incrementing number of routees with 1
            actor ! "hit"
            Await.result(actor ? CurrentRoutees, remaining).asInstanceOf[RouterRoutees].routees.size must be(n)
            expectMsgType[ActorRef]
          }).toSet

        enterBarrier("broadcast-end")
        actor ! Broadcast(PoisonPill)

        enterBarrier("end")
        repliesFrom.size must be(7)
        val repliesFromAddresses = repliesFrom.map(_.path.address)
        repliesFromAddresses must be === (Set(node(first), node(second), node(third)).map(_.address))

        // shut down the actor before we let the other node(s) shut down so we don't try to send
        // "Terminate" to a shut down node
        system.stop(actor)
      }

      enterBarrier("done")
    }
  }

  "A remote round robin nozzle" must {
    "send messages with actor selection to remote paths" taggedAs LongRunningTest in {

      runOn(first, second, third) {
        system.actorOf(Props[SomeActor], name = "target-" + myself.name)
        enterBarrier("start", "end")
      }

      runOn(fourth) {
        enterBarrier("start")
        val actor = system.actorOf(Props.empty.withRouter(FromConfig), "service-hello3")
        actor.isInstanceOf[RoutedActorRef] must be(true)

        val connectionCount = 3
        val iterationCount = 10

        for (i ← 0 until iterationCount; k ← 0 until connectionCount) {
          actor ! "hit"
        }

        val replies: Map[Address, Int] = (receiveWhile(5 seconds, messages = connectionCount * iterationCount) {
          case ref: ActorRef ⇒ ref.path.address
        }).foldLeft(Map(node(first).address -> 0, node(second).address -> 0, node(third).address -> 0)) {
          case (replyMap, address) ⇒ replyMap + (address -> (replyMap(address) + 1))
        }

        enterBarrier("end")
        replies.values foreach { _ must be(iterationCount) }
        replies.get(node(fourth).address) must be(None)
      }

      enterBarrier("done")
    }
  }
}
