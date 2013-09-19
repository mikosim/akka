/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing2

import akka.routing.RouterConfig
import akka.actor.Props
import akka.routing.Route
import akka.actor.ActorContext
import akka.routing.RouteeProvider
import akka.routing.Resizer
import akka.actor.ActorPath
import akka.actor.SupervisorStrategy
import akka.actor.Actor
import akka.actor.OneForOneStrategy

object RouterConfig2 {
  val defaultSupervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _ ⇒ SupervisorStrategy.Escalate
  }
}

// FIXME #3549
trait RouterConfig2 extends RouterConfig {

  def createRouter(context: ActorContext, routeeProps: Props): Router

  override def createRoute(routeeProvider: RouteeProvider): Route = ???

  override def createRouteeProvider(context: ActorContext, routeeProps: Props): RouteeProvider = ???

  override def createActor(): Actor = new RouterActor {
    override def supervisorStrategy: SupervisorStrategy = RouterConfig2.this.supervisorStrategy
  }

  override def supervisorStrategy: SupervisorStrategy = RouterConfig2.defaultSupervisorStrategy

  override def routerDispatcher: String = ???

  override def withFallback(other: RouterConfig): RouterConfig = this

  override def resizer: Option[Resizer] = ???

  override def verifyConfig(path: ActorPath): Unit = ()

  override def stopRouterWhenAllRouteesRemoved: Boolean = ???

}