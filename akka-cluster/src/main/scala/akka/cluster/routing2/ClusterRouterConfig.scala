/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster.routing2

import scala.collection.immutable
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
import akka.cluster.routing.ClusterRouterSettings
import akka.routing2.Nozzle
import akka.remote.routing2.RemoteRouterConfig
import akka.routing2.RouterActor
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.actor.ActorRef
import akka.cluster.Member
import scala.annotation.tailrec
import akka.actor.RootActorPath
import akka.cluster.MemberStatus
import akka.routing2.ActorSelectionRoutee

/**
 * [[akka.routing.RouterConfig]] implementation for deployment on cluster nodes.
 * Delegates other duties to the local [[akka.routing.RouterConfig]],
 * which makes it possible to mix this with the built-in routers such as
 * [[akka.routing.RoundRobinRouter]] or custom routers.
 */
@SerialVersionUID(1L)
final case class ClusterNozzle(local: Nozzle, settings: ClusterRouterSettings) extends Nozzle with ClusterRouterConfig {

  require(settings.routeesPath.nonEmpty, "routeesPath must be defined")

  override def paths: immutable.Iterable[String] = Nil

  override def withFallback(other: RouterConfig): RouterConfig = other match {
    case ClusterNozzle(_: ClusterNozzle, _) ⇒ throw new IllegalStateException(
      "ClusterNozzle is not allowed to wrap a ClusterNozzle")
    case ClusterNozzle(local, _) ⇒
      // FIXME #3549 is this always correct?
      copy(local = this.local.withFallback(local).asInstanceOf[Nozzle])
    case _ ⇒
      copy(local = this.local.withFallback(other).asInstanceOf[Nozzle])
  }

}

/**
 * [[akka.routing.RouterConfig]] implementation for deployment on cluster nodes.
 * Delegates other duties to the local [[akka.routing.RouterConfig]],
 * which makes it possible to mix this with the built-in routers such as
 * [[akka.routing.RoundRobinRouter]] or custom routers.
 */
@SerialVersionUID(1L)
final case class ClusterPool(local: Pool, settings: ClusterRouterSettings) extends Pool with ClusterRouterConfig {

  require(local.resizer2.isEmpty, "Resizer can't be used together with cluster router")

  private val childNameCounter = new AtomicInteger

  /**
   * INTERNAL API
   */
  override private[akka] def newRoutee(routeeProps: Props, context: ActorContext): Routee = {
    val name = "c" + childNameCounter.incrementAndGet
    val ref = context.asInstanceOf[ActorCell].attachChild(routeeProps, name, systemService = false)
    ActorRefRoutee(ref)
  }

  /**
   * Initial number of routee instances
   */
  override def nrOfInstances: Int = 0

  override def resizer2: Option[Resizer] = local.resizer2

  override def withFallback(other: RouterConfig): RouterConfig = other match {
    case ClusterPool(_: ClusterPool, _) ⇒ throw new IllegalStateException(
      "ClusterPool is not allowed to wrap a ClusterPool")
    case ClusterPool(local, _) ⇒
      // FIXME #3549 is this always correct?
      copy(local = this.local.withFallback(local).asInstanceOf[Pool])
    case _ ⇒
      copy(local = this.local.withFallback(other).asInstanceOf[Pool])
  }

}

private[akka] trait ClusterRouterConfig extends RouterConfig2 {
  def local: RouterConfig2
  def settings: ClusterRouterSettings
  override def createRouter(): Router = local.createRouter()
  override def createActor(): Actor = new ClusterRouterActor(local.supervisorStrategy, settings)
  override def supervisorStrategy: SupervisorStrategy = local.supervisorStrategy
  override def routerDispatcher: String = local.routerDispatcher
  override def stopRouterWhenAllRouteesRemoved: Boolean = false

  // Intercept ClusterDomainEvent and route them to the ClusterRouterActor
  override def isManagementMessage(msg: Any): Boolean =
    (msg.isInstanceOf[ClusterDomainEvent]) || super.isManagementMessage(msg)
}

/**
 * INTERNAL API
 * The router actor, subscribes to cluster events and
 * adjusts the routees.
 */
private[akka] class ClusterRouterActor(override val supervisorStrategy: SupervisorStrategy,
                                       settings: ClusterRouterSettings) extends RouterActor {

  def cluster: Cluster = Cluster(context.system)

  // re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
    cluster.subscribe(self, classOf[ReachabilityEvent])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  var nodes: immutable.SortedSet[Address] = {
    import Member.addressOrdering
    cluster.readView.members.collect {
      case m if isAvailable(m) ⇒ m.address
    }
  }

  def isAvailable(m: Member): Boolean =
    m.status == MemberStatus.Up &&
      satisfiesRole(m.roles) &&
      (settings.allowLocalRoutees || m.address != cluster.selfAddress)

  private def satisfiesRole(memberRoles: Set[String]): Boolean = settings.useRole match {
    case None    ⇒ true
    case Some(r) ⇒ memberRoles.contains(r)
  }

  def availableNodes: immutable.SortedSet[Address] = {
    import Member.addressOrdering
    val currentNodes = nodes
    if (currentNodes.isEmpty && settings.allowLocalRoutees && satisfiesRole(cluster.selfRoles))
      //use my own node, cluster information not updated yet
      immutable.SortedSet(cluster.selfAddress)
    else
      currentNodes
  }

  /**
   * Fills in self address for local ActorRef
   */
  def fullAddress(routee: Routee): Address = {
    val a = routee match {
      case ActorRefRoutee(ref)             ⇒ ref.path.address
      case ActorSelectionRoutee(selection) ⇒ selection.anchor.path.address
    }
    a match {
      case Address(_, _, None, None) ⇒ cluster.selfAddress
      case a                         ⇒ a
    }
  }

  /**
   * Adds routees based on totalInstances and maxInstancesPerNode settings
   */
  def addRoutees(): Unit = {
    @tailrec
    def doAddRoutees(): Unit = selectDeploymentTarget match {
      case None ⇒ // done
      case Some(target) ⇒
        val routee = cell.routerConfig match {
          case pool: Pool ⇒
            val routeeProps = cell.routeeProps
            val deploy = Deploy(config = ConfigFactory.empty(), routerConfig = routeeProps.routerConfig,
              scope = RemoteScope(target))
            pool.newRoutee(routeeProps.withDeploy(deploy), context)
          case nozzle: Nozzle ⇒
            nozzle.routeeFor(target + settings.routeesPath, context)
          case _ ⇒
            // FIXME #3549 precondition
            throw new IllegalStateException
        }
        // must register each one, since registered routees are used in selectDeploymentTarget
        cell.addRoutee(routee)

        // recursion until all created
        doAddRoutees()
    }

    doAddRoutees()
  }

  private def selectDeploymentTarget: Option[Address] = {
    val currentRoutees = cell.router.routees
    val currentNodes = availableNodes
    if (currentNodes.isEmpty || currentRoutees.size >= settings.totalInstances) {
      None
    } else {
      // find the node with least routees
      val numberOfRouteesPerNode: Map[Address, Int] =
        currentRoutees.foldLeft(currentNodes.map(_ -> 0).toMap.withDefaultValue(0)) { (acc, x) ⇒
          val address = fullAddress(x)
          acc + (address -> (acc(address) + 1))
        }

      val (address, count) = numberOfRouteesPerNode.minBy(_._2)
      if (count < settings.maxInstancesPerNode) Some(address) else None
    }
  }

  def addMember(member: Member) = {
    nodes += member.address
    addRoutees()
  }

  def removeMember(member: Member) = {
    val address = member.address
    nodes -= address

    // unregister routees that live on that node
    val affectedRoutees = cell.router.routees.filter(fullAddress(_) == address)
    cell.removeRoutees(affectedRoutees, stopChild = true)

    // addRoutees will not create more than createRoutees and maxInstancesPerNode
    // this is useful when totalInstances < upNodes.size
    addRoutees()
  }

  override def receive = clusterReceive orElse super.receive

  def clusterReceive: Receive = {
    case s: CurrentClusterState ⇒
      import Member.addressOrdering
      nodes = s.members.collect { case m if isAvailable(m) ⇒ m.address }
      addRoutees()

    case m: MemberEvent if isAvailable(m.member) ⇒
      addMember(m.member)

    case other: MemberEvent ⇒
      // other events means that it is no longer interesting, such as
      // MemberExited, MemberRemoved
      removeMember(other.member)

    case UnreachableMember(m) ⇒
      removeMember(m)

    case ReachableMember(m) ⇒
      if (isAvailable(m)) addMember(m)
  }
}