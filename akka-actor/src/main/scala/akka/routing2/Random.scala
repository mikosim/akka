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
import akka.actor.SupervisorStrategy
import akka.routing.RouterConfig
import akka.japi.Util.immutableSeq

object RandomRoutingLogic {
  def apply(): RandomRoutingLogic = new RandomRoutingLogic
}

class RandomRoutingLogic extends RoutingLogic {
  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee =
    if (routees.isEmpty) NoRoutee
    else routees(ThreadLocalRandom.current.nextInt(routees.size))
}

final case class RandomPool(
  override val nrOfInstances: Int, override val resizer2: Option[Resizer] = None,
  override val supervisorStrategy: SupervisorStrategy = RouterConfig2.defaultSupervisorStrategy,
  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId)
  extends Pool with PoolOverrideUnsetConfig[RandomPool] {

  def this(config: Config) =
    this(
      nrOfInstances = config.getInt("nr-of-instances"),
      resizer2 = DefaultResizer.fromConfig(config))

  override def createRouter(): Router = new Router(RandomRoutingLogic())

  /**
   * Setting the supervisor strategy to be used for the “head” Router actor.
   */
  def withSupervisorStrategy(strategy: SupervisorStrategy): RandomPool = copy(supervisorStrategy = strategy)

  /**
   * Setting the resizer to be used.
   */
  def withResizer(resizer: Resizer): RandomPool = copy(resizer2 = Some(resizer))
}

final case class RandomNozzle(
  paths: immutable.Iterable[String],
  override val supervisorStrategy: SupervisorStrategy = RouterConfig2.defaultSupervisorStrategy,
  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId)
  extends Nozzle with NozzleOverrideUnsetConfig[RandomNozzle] {

  def this(config: Config) =
    this(paths = immutableSeq(config.getStringList("routees.paths")))

  override def createRouter(): Router = new Router(RandomRoutingLogic())

  /**
   * Setting the supervisor strategy to be used for the “head” Router actor.
   */
  override def withSupervisorStrategy(strategy: SupervisorStrategy): RandomNozzle = copy(supervisorStrategy = strategy)

  /**
   * Uses the resizer and/or the supervisor strategy of the given Routerconfig
   * if this RouterConfig doesn't have one, i.e. the resizer defined in code is used if
   * resizer was not defined in config.
   */
  override def withFallback(other: RouterConfig): RouterConfig = this.overrideUnsetConfig(other)

}
