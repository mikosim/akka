/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing2

import scala.collection.immutable
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorCell
import akka.actor.ActorInitializationException
import akka.actor.ActorSystemImpl
import akka.actor.AutoReceivedMessage
import akka.actor.IndirectActorProducer
import akka.actor.InternalActorRef
import akka.actor.Props
import akka.actor.Terminated
import akka.dispatch.Envelope
import akka.dispatch.MessageDispatcher
import akka.routing.RouterConfig

/**
 * INTERNAL API
 */
private[akka] object RoutedActorCell {
  class RouterActorCreator(routerConfig: RouterConfig) extends IndirectActorProducer {
    override def actorClass = classOf[RouterActor]
    override def produce() = routerConfig.createActor()
  }

}

/**
 * INTERNAL API
 */
private[akka] trait RouterManagementMesssage

/**
 * INTERNAL API
 */
private[akka] class RoutedActorCell(
  _system: ActorSystemImpl,
  _ref: InternalActorRef,
  _routerProps: Props,
  _routerDispatcher: MessageDispatcher,
  val routeeProps: Props,
  _supervisor: InternalActorRef)
  extends ActorCell(_system, _ref, _routerProps, _routerDispatcher, _supervisor) {

  // FIXME #3549 remove cast
  private[akka] val routerConfig = _routerProps.routerConfig.asInstanceOf[RouterConfig2]

  @volatile
  protected var _router: Router = null // initialized in start
  def router: Router = _router

  override def start(): this.type = {
    // create the initial routees before scheduling the Router actor
    _router = routerConfig match {
      case init: CreateInitialChildRoutees ⇒
        val r = routerConfig.createRouter()
        val routees = immutable.IndexedSeq.fill(init.nrOfInstances)(ActorRefRoutee(this.actorOf(routeeProps)))
        r.withRoutees(routees)
      case _ ⇒ routerConfig.createRouter()
    }
    preSuperStart()
    super.start()
  }

  protected def preSuperStart(): Unit = ()

  /*
   * end of construction
   */

  /**
   * Send the message to the destinations defined by the `route` function.
   *
   * If the message is a [[akka.routing.RouterEnvelope]] it will be
   * unwrapped before sent to the destinations.
   *
   * When [[akka.routing.CurrentRoutees]] is sent to the RoutedActorRef it
   * replies with [[akka.routing.RouterRoutees]].
   *
   * Resize is triggered when messages are sent to the routees, and the
   * resizer is invoked asynchronously, i.e. not necessarily before the
   * message has been sent.
   */
  override def sendMessage(envelope: Envelope): Unit = {
    envelope.message match {
      case _: AutoReceivedMessage | _: Terminated | _: RouterManagementMesssage ⇒
        super.sendMessage(envelope)
      case msg ⇒
        router.route(msg, envelope.sender)
    }

  }

}

/**
 * INTERNAL API
 */
private[akka] class RouterActor extends Actor {

  val cell = context match {
    case x: RoutedActorCell ⇒ x
    case _                  ⇒ throw ActorInitializationException("Router actor can only be used in RoutedActorRef, not in " + context.getClass)
  }

  def receive = {
    // FIXME #3549 watch/remove routee, stopRouterWhenAllRouteesRemoved
    case Terminated(child) ⇒
    case CurrentRoutees ⇒
      sender ! RouterRoutees(cell.router.routees)

  }

  override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {
    // do not scrap children
  }
}