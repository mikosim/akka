/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing2

import scala.collection.immutable
import akka.actor.ActorRef
import akka.actor.ActorSelection

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

case class ActorSelectionRoutee(selection: ActorSelection) extends Routee {
  override def send(message: Any, sender: ActorRef): Unit =
    selection.tell(message, sender)
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

  def addRoutee(routee: Routee): Router =
    new Router(routees :+ routee, logic)

  def removeRoutee(routee: Routee): Router =
    new Router(routees = routees.filterNot(_ == routee), logic)

}

