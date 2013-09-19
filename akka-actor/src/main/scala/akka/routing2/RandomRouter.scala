/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing2

import scala.collection.immutable
import scala.concurrent.forkjoin.ThreadLocalRandom
import akka.actor.ActorContext
import akka.actor.Props
import akka.dispatch.Dispatchers
import com.typesafe.config.Config

object RandomRoutingLogic {
  def apply(): RandomRoutingLogic = new RandomRoutingLogic
}

class RandomRoutingLogic extends RoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee =
    if (routees.isEmpty) NoRoutee
    else routees(ThreadLocalRandom.current.nextInt(routees.size))
}

case class RandomRouter(override val nrOfInstances: Int, override val resizer2: Option[Resizer] = None)
  extends RouterConfig2 with CreateInitialChildRoutees with Resizable {

  def this(config: Config) =
    this(
      nrOfInstances = config.getInt("nr-of-instances"),
      resizer2 = DefaultResizer.fromConfig(config))

  override def createRouter(): Router =
    new Router(Vector.empty, RandomRoutingLogic())

  // FIXME #3549 routerDispatcher and supervisorStrategy in constructor
  override def routerDispatcher: String = Dispatchers.DefaultDispatcherId
}