/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing2

import java.util.concurrent.atomic.AtomicLong
import scala.collection.immutable
import akka.actor.ActorContext
import akka.actor.Props
import akka.dispatch.Dispatchers
import com.typesafe.config.Config

object RoundRobinRoutingLogic {
  def apply(): RoundRobinRoutingLogic = new RoundRobinRoutingLogic
}

class RoundRobinRoutingLogic extends RoutingLogic {
  val next = new AtomicLong(0)

  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee =
    if (routees.isEmpty) NoRoutee
    else routees((next.getAndIncrement % routees.size).asInstanceOf[Int])

}

case class RoundRobinRouter(nrOfInstances: Int) extends RouterConfig2 {

  def this(config: Config) =
    this(config.getInt("nr-of-instances"))

  override def createRouter(context: ActorContext, routeeProps: Props): Router = {
    val routees = immutable.IndexedSeq.fill(nrOfInstances)(ActorRefRoutee(context.actorOf(routeeProps)))
    new Router(routees, RoundRobinRoutingLogic())
  }

  // FIXME #3549 routerDispatcher and supervisorStrategy in constructor
  override def routerDispatcher: String = Dispatchers.DefaultDispatcherId
}