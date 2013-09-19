/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing2

import scala.collection.immutable
import java.util.concurrent.atomic.AtomicLong
import akka.actor.ActorRef
import akka.actor.ActorContext
import akka.actor.Props
import akka.dispatch.Dispatchers
import akka.actor.SupervisorStrategy
import akka.actor.OneForOneStrategy
import akka.ConfigurationException
import akka.actor.ActorPath
import akka.actor.Actor

trait RoutingLogic {
  def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee
}

trait Routee {
  def send(message: Any, sender: ActorRef): Unit
}

case class ActorRefRoutee(ref: ActorRef) extends Routee {
  override def send(message: Any, sender: ActorRef): Unit =
    ref.tell(message, sender)
}

object NoRoutee extends Routee {
  // TODO #3549 not deadLetters any more?
  override def send(message: Any, sender: ActorRef): Unit = ()
}

case class SeveralRoutees(routees: immutable.IndexedSeq[Routee]) extends Routee {
  override def send(message: Any, sender: ActorRef): Unit =
    routees.foreach(_.send(message, sender))
}

final class Router(val routees: immutable.IndexedSeq[Routee], val logic: RoutingLogic) {

  def route(message: Any, sender: ActorRef): Unit =
    message match {
      case akka.routing.Broadcast(msg) ⇒ SeveralRoutees(routees).send(msg, sender)
      case msg                         ⇒ logic.select(msg, routees).send(msg, sender)
    }

  def withRoutees(rs: immutable.IndexedSeq[Routee]): Router = new Router(rs, logic)

}

/**
 * Router configuration which has no default, i.e. external configuration is required.
 */
case object FromConfig extends FromConfig {
  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
  @inline final def apply(routerDispatcher: String = Dispatchers.DefaultDispatcherId) = new FromConfig(routerDispatcher)
  @inline final def unapply(fc: FromConfig): Option[String] = Some(fc.routerDispatcher)
}

/**
 * Java API: Router configuration which has no default, i.e. external configuration is required.
 *
 * This can be used when the dispatcher to be used for the head Router needs to be configured
 * (defaults to default-dispatcher).
 */
@SerialVersionUID(1L)
class FromConfig(override val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
                 override val supervisorStrategy: SupervisorStrategy = RouterConfig2.defaultSupervisorStrategy)
  extends RouterConfig2 with Serializable {

  def this() = this(Dispatchers.DefaultDispatcherId, RouterConfig2.defaultSupervisorStrategy)

  override def createRouter(): Router =
    throw new UnsupportedOperationException("FromConfig must not create Router")

  override def verifyConfig(path: ActorPath): Unit =
    throw new ConfigurationException(s"Configuration missing for router [$path] in 'akka.actor.deployment' section.")

  /**
   * Java API for setting the supervisor strategy to be used for the “head”
   * Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): FromConfig = new FromConfig(this.routerDispatcher, strategy)
}

/**
 * Routing configuration that indicates no routing; this is also the default
 * value which hence overrides the merge strategy in order to accept values
 * from lower-precedence sources. The decision whether or not to create a
 * router is taken in the LocalActorRefProvider based on Props.
 */
@SerialVersionUID(1L)
abstract class NoRouter extends RouterConfig2
case object NoRouter extends NoRouter {
  override def createRouter(): Router = throw new UnsupportedOperationException("NoRouter has no Router")
  override def routerDispatcher: String = throw new UnsupportedOperationException("NoRouter has no dispatcher")
  override def supervisorStrategy = throw new UnsupportedOperationException("NoRouter has no strategy")
  override def withFallback(other: akka.routing.RouterConfig): akka.routing.RouterConfig = other

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

trait CreateInitialChildRoutees {
  def nrOfInstances: Int
}

/**
 * Sending this message to a router will make it send back its currently used routees.
 * A RouterRoutees message is sent asynchronously to the "requester" containing information
 * about what routees the router is routing over.
 */
abstract class CurrentRoutees extends RouterManagementMesssage
@SerialVersionUID(1L)
case object CurrentRoutees extends CurrentRoutees {
  /**
   * Java API: get the singleton instance
   */
  def getInstance = this
}

/**
 * Message used to carry information about what routees the router is currently using.
 */
@SerialVersionUID(1L)
case class RouterRoutees(routees: immutable.IndexedSeq[Routee]) {
  /**
   * Java API
   */
  def getRoutees: java.util.List[Routee] = {
    import scala.collection.JavaConverters._
    routees.asJava
  }
}
