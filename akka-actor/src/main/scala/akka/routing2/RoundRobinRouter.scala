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

case class RoundRobinRouter(override val nrOfInstances: Int, override val resizer2: Option[Resizer] = None)
  extends RouterConfig2 with CreateInitialChildRoutees with Resizable {

  def this(config: Config) =
    this(
      nrOfInstances = config.getInt("nr-of-instances"),
      resizer2 = DefaultResizer.fromConfig(config))

  override def createRouter(): Router =
    new Router(Vector.empty, RoundRobinRoutingLogic())

}