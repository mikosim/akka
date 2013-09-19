/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing2

import akka.ConfigurationException
import akka.actor.Actor
import akka.actor.ActorCell
import akka.actor.ActorInitializationException
import akka.actor.ActorPath
import akka.actor.ActorSystemImpl
import akka.actor.AutoReceivedMessage
import akka.actor.Cell
import akka.actor.IndirectActorProducer
import akka.actor.InternalActorRef
import akka.actor.Props
import akka.actor.RepointableActorRef
import akka.actor.Terminated
import akka.actor.UnstartedCell
import akka.dispatch.BalancingDispatcher
import akka.dispatch.Envelope
import akka.dispatch.MailboxType
import akka.dispatch.MessageDispatcher
import akka.routing.NoRouter
import akka.routing.RouterConfig
import akka.routing.RouterEnvelope

/**
 * INTERNAL API
 *
 * A RoutedActorRef is an ActorRef that has a set of connected ActorRef and it uses a Router to
 * send a message to one (or more) of these actors.
 */
private[akka] class RoutedActorRef(
  _system: ActorSystemImpl,
  _routerProps: Props,
  _routerDispatcher: MessageDispatcher,
  _routerMailbox: MailboxType,
  _routeeProps: Props,
  _supervisor: InternalActorRef,
  _path: ActorPath)
  extends RepointableActorRef(_system, _routerProps, _routerDispatcher, _routerMailbox, _supervisor, _path) {

  // verify that a BalancingDispatcher is not used with a Router
  if (_routerProps.routerConfig != NoRouter && _routerDispatcher.isInstanceOf[BalancingDispatcher]) {
    throw new ConfigurationException(
      "Configuration for " + this +
        " is invalid - you can not use a 'BalancingDispatcher' as a Router's dispatcher, you can however use it for the routees.")
  } else _routerProps.routerConfig.verifyConfig(_path)

  override def newCell(old: UnstartedCell): Cell =
    new RoutedActorCell(system, this, props, dispatcher, _routeeProps, supervisor)
      .init(sendSupervise = false, mailboxType)

}

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
private[akka] final class RoutedActorCell(
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
  private var _router: Router = null // initialized in start
  def router: Router = _router

  override def start(): this.type = {
    // create the routees before scheduling the Router actor
    _router = routerConfig.createRouter(this, routeeProps)
    super.start()
  }

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
    // FIXME #3549 CurrentRoutees
    envelope.message match {
      case _: AutoReceivedMessage | _: Terminated ⇒
        super.sendMessage(envelope)
      case msg ⇒
        router.route(msg, envelope.sender)
    }

  }

}

/**
 * INTERNAL API
 */
private[akka] trait RouterActor extends Actor {

  val ref = context match {
    case x: RoutedActorCell ⇒ x
    case _                  ⇒ throw ActorInitializationException("Router actor can only be used in RoutedActorRef, not in " + context.getClass)
  }

  final def receive = {
    // FIXME #3549 watch/remove routee, stopRouterWhenAllRouteesRemoved
    case Terminated(child) ⇒

  }

  override def preRestart(cause: Throwable, msg: Option[Any]): Unit = {
    // do not scrap children
  }
}