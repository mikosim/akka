/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing2

import scala.collection.immutable
import akka.ConfigurationException
import akka.actor.Actor
import akka.actor.ActorContext
import akka.actor.ActorPath
import akka.actor.AutoReceivedMessage
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.actor.Terminated
import akka.dispatch.Dispatchers
import akka.routing.Route
import akka.routing.RouteeProvider
import akka.routing.RouterConfig

object RouterConfig2 {
  val defaultSupervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ ⇒ SupervisorStrategy.Escalate
  }
}

// FIXME #3549 this will be the new RouterConfig
/**
 * This trait represents a router factory: it produces the actual router actor
 * and creates the routing table (a function which determines the recipients
 * for each message which is to be dispatched). The resulting RoutedActorRef
 * optimizes the sending of the message so that it does NOT go through the
 * router’s mailbox unless the route returns an empty recipient set.
 *
 * '''Caution:''' This means
 * that the route function is evaluated concurrently without protection by
 * the RoutedActorRef: either provide a reentrant (i.e. pure) implementation or
 * do the locking yourself!
 *
 * '''Caution:''' Please note that the [[akka.routing.Router]] which needs to
 * be returned by `createActor()` should not send a message to itself in its
 * constructor or `preStart()` or publish its self reference from there: if
 * someone tries sending a message to that reference before the constructor of
 * RoutedActorRef has returned, there will be a `NullPointerException`!
 */
trait RouterConfig2 extends RouterConfig {

  /**
   * Create the actual router, responsible for routing messages to routees.
   */
  def createRouter(): Router

  // FIXME #3549 change signature to `createRouterActor: RouterActor`
  /**
   * The router "head" actor.
   */
  override def createActor(): Actor = new RouterActor {
    override def supervisorStrategy: SupervisorStrategy = RouterConfig2.this.supervisorStrategy
  }

  /**
   * Is the message handled by the router head actor
   */
  def isManagementMessage(msg: Any): Boolean = msg match {
    case _: AutoReceivedMessage | _: Terminated | _: RouterManagementMesssage ⇒ true
    case _ ⇒ false
  }

  /*
   * Specify that this router should stop itself when all routees have terminated (been removed).
   * By Default it is `true`, unless a `resizer` is used.
   */
  override def stopRouterWhenAllRouteesRemoved: Boolean = true

  /**
   * Overridable merge strategy, by default completely prefers “this” (i.e. no merge).
   */
  override def withFallback(other: RouterConfig): RouterConfig = this

  /**
   * Check that everything is there which is needed. Called in constructor of RoutedActorRef to fail early.
   */
  override def verifyConfig(path: ActorPath): Unit = ()

  // FIXME #3549 remove these 
  override def createRoute(routeeProvider: RouteeProvider): Route = ???
  override def createRouteeProvider(context: ActorContext, routeeProps: Props): RouteeProvider = ???
  override def resizer: Option[akka.routing.Resizer] = ???

}

/**
 * INTERNAL API
 *
 * Used to override unset configuration in a router.
 */
private[akka] trait NozzleOverrideUnsetConfig[T <: Nozzle] extends Nozzle {

  final def overrideUnsetConfig(other: RouterConfig): RouterConfig =
    if (other == NoRouter) this // NoRouter is the default, hence “neutral”
    else if ((this.supervisorStrategy eq RouterConfig2.defaultSupervisorStrategy)
      && (other.supervisorStrategy ne RouterConfig2.defaultSupervisorStrategy))
      this.withSupervisorStrategy(other.supervisorStrategy).asInstanceOf[NozzleOverrideUnsetConfig[T]]
    else this

  def withSupervisorStrategy(strategy: SupervisorStrategy): T
}

/**
 * INTERNAL API
 *
 * Used to override unset configuration in a router.
 */
private[akka] trait PoolOverrideUnsetConfig[T <: Pool] extends Pool {

  final def overrideUnsetConfig(other: RouterConfig): RouterConfig =
    if (other == NoRouter) this // NoRouter is the default, hence “neutral”
    else {
      val wssConf: PoolOverrideUnsetConfig[T] =
        if ((this.supervisorStrategy eq RouterConfig2.defaultSupervisorStrategy)
          && (other.supervisorStrategy ne RouterConfig2.defaultSupervisorStrategy))
          this.withSupervisorStrategy(other.supervisorStrategy).asInstanceOf[PoolOverrideUnsetConfig[T]]
        else this

      other match {
        case r: Pool if wssConf.resizer2.isEmpty && r.resizer2.isDefined ⇒
          wssConf.withResizer(r.resizer2.get)
        case _ ⇒ wssConf
      }
    }

  def withSupervisorStrategy(strategy: SupervisorStrategy): T

  def withResizer(resizer: Resizer): T
}

trait Nozzle extends RouterConfig2 {

  def paths: immutable.Iterable[String]

  /**
   * INTERNAL API
   */
  private[akka] def routeeFor(path: String, context: ActorContext): Routee =
    ActorSelectionRoutee(context.actorSelection(path))
}

trait Pool extends RouterConfig2 {
  /**
   * Initial number of routee instances
   */
  def nrOfInstances: Int

  /**
   * INTERNAL API
   */
  private[akka] def newRoutee(routeeProps: Props, context: ActorContext): Routee =
    ActorRefRoutee(context.actorOf(routeeProps))

  // FIXME #3549 signature clash with old resizer method
  /**
   * Pool with dynamically resizable number of routees return the [[akka.routing.Resizer]]
   * to use. The resizer is invoked once when the router is created, before any messages can
   * be sent to it. Resize is also triggered when messages are sent to the routees, and the
   * resizer is invoked asynchronously, i.e. not necessarily before the message has been sent.
   */
  def resizer2: Option[Resizer]

  /*
   * Specify that this router should stop itself when all routees have terminated (been removed).
   * By Default it is `true`, unless a `resizer` is used.
   */
  override def stopRouterWhenAllRouteesRemoved: Boolean = resizer2.isEmpty

  override def createActor(): Actor =
    resizer2 match {
      case Some(r) ⇒
        new ResizablePoolActor {
          override def supervisorStrategy: SupervisorStrategy = super.supervisorStrategy
        }
      case _ ⇒ super.createActor()
    }
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

/**
 * INTERNAL API
 */
private[akka] trait RouterManagementMesssage

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
