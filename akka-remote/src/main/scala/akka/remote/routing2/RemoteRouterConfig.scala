/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.remote.routing2

import akka.routing2.RouterConfig2
import akka.routing2.Router
import akka.actor.Props
import akka.actor.ActorContext
import akka.routing2.Routee
import java.util.concurrent.atomic.AtomicInteger
import akka.actor.Address
import akka.actor.ActorCell
import akka.actor.Deploy
import com.typesafe.config.ConfigFactory
import akka.routing2.ActorRefRoutee
import akka.remote.RemoteScope
import akka.actor.Actor
import akka.actor.SupervisorStrategy
import akka.routing2.Resizer
import akka.routing.RouterConfig
import akka.routing2.Pool

final case class RemoteRouterConfig(local: Pool, nodes: Iterable[Address])
  extends Pool {

  require(nodes.nonEmpty, "Must specify list of remote target.nodes")

  // need this iterator as instance variable since Resizer may call createRoutees several times
  private val nodeAddressIter: Iterator[Address] = Stream.continually(nodes).flatten.iterator
  // need this counter as instance variable since Resizer may call createRoutees several times
  private val childNameCounter = new AtomicInteger

  override def createRouter(): Router = local.createRouter()

  override def nrOfInstances: Int = local.nrOfInstances

  override def newRoutee(routeeProps: Props, context: ActorContext): Routee = {
    val name = "c" + childNameCounter.incrementAndGet
    val deploy = Deploy(config = ConfigFactory.empty(), routerConfig = routeeProps.routerConfig,
      scope = RemoteScope(nodeAddressIter.next))

    // attachChild means that the provider will treat this call as if possibly done out of the wrong
    // context and use RepointableActorRef instead of LocalActorRef. Seems like a slightly sub-optimal
    // choice in a corner case (and hence not worth fixing).
    val ref = context.asInstanceOf[ActorCell].attachChild(routeeProps.withDeploy(deploy), name, systemService = false)
    ActorRefRoutee(ref)
  }

  override def createActor(): Actor = local.createActor()

  override def supervisorStrategy: SupervisorStrategy = local.supervisorStrategy

  override def routerDispatcher: String = local.routerDispatcher

  override def resizer2: Option[Resizer] = local.resizer2

  override def withFallback(other: RouterConfig): RouterConfig = other match {
    case RemoteRouterConfig(local: RemoteRouterConfig, nodes) ⇒ throw new IllegalStateException(
      "RemoteRouterConfig is not allowed to wrap a RemoteRouterConfig")
    case RemoteRouterConfig(local: Pool, nodes) ⇒
      // FIXME #3549 is this always correct?
      copy(local = this.local.withFallback(local).asInstanceOf[Pool])
    case _ ⇒ copy(local = this.local.withFallback(other).asInstanceOf[Pool])
  }

}