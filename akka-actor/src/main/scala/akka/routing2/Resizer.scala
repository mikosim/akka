/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.routing2

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config

import akka.actor.Actor
import akka.actor.ActorCell
import akka.actor.ActorInitializationException
import akka.actor.ActorRefWithCell
import akka.actor.ActorSystemImpl
import akka.actor.InternalActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.dispatch.Envelope
import akka.dispatch.MessageDispatcher

/**
 * Routers with dynamically resizable number of routees is implemented by providing a Resizer
 * implementation in [[akka.routing.RouterConfig]].
 */
trait Resizer {
  /**
   * Is it time for resizing. Typically implemented with modulo of nth message, but
   * could be based on elapsed time or something else. The messageCounter starts with 0
   * for the initial resize and continues with 1 for the first message. Make sure to perform
   * initial resize before first message (messageCounter == 0), because there is no guarantee
   * that resize will be done when concurrent messages are in play.
   *
   * CAUTION: this method is invoked from the thread which tries to send a
   * message to the pool, i.e. the ActorRef.!() method, hence it may be called
   * concurrently.
   */
  def isTimeForResize(messageCounter: Long): Boolean

  /**
   * Decide if the capacity of the router need to be changed. Will be invoked when `isTimeForResize`
   * returns true and no other resize is in progress.
   * Create and register more routees with `routeeProvider.registerRoutees(newRoutees)
   * or remove routees with `routeeProvider.unregisterRoutees(abandonedRoutees)` and
   * sending [[akka.actor.PoisonPill]] to them.
   *
   * This method is invoked only in the context of the Router actor in order to safely
   * create/stop children.
   */
  def resize(currentRoutees: immutable.IndexedSeq[Routee]): Int
}

case object DefaultResizer {

  /**
   * Creates a new DefaultResizer from the given configuration
   */
  def apply(resizerConfig: Config): DefaultResizer =
    DefaultResizer(
      lowerBound = resizerConfig.getInt("lower-bound"),
      upperBound = resizerConfig.getInt("upper-bound"),
      pressureThreshold = resizerConfig.getInt("pressure-threshold"),
      rampupRate = resizerConfig.getDouble("rampup-rate"),
      backoffThreshold = resizerConfig.getDouble("backoff-threshold"),
      backoffRate = resizerConfig.getDouble("backoff-rate"),
      stopDelay = Duration(resizerConfig.getMilliseconds("stop-delay"), TimeUnit.MILLISECONDS),
      messagesPerResize = resizerConfig.getInt("messages-per-resize"))

  def fromConfig(resizerConfig: Config): Option[DefaultResizer] =
    if (resizerConfig.getBoolean("resizer.enabled"))
      Some(DefaultResizer(resizerConfig.getConfig("resizer")))
    else
      None
}

//FIXME DOCUMENT ME
@SerialVersionUID(1L)
case class DefaultResizer(
  /**
   * The fewest number of routees the router should ever have.
   */
  lowerBound: Int = 1,
  /**
 * The most number of routees the router should ever have.
 * Must be greater than or equal to `lowerBound`.
 */
  upperBound: Int = 10,
  /**
 * Threshold to evaluate if routee is considered to be busy (under pressure).
 * Implementation depends on this value (default is 1).
 * <ul>
 * <li> 0:   number of routees currently processing a message.</li>
 * <li> 1:   number of routees currently processing a message has
 *           some messages in mailbox.</li>
 * <li> > 1: number of routees with at least the configured `pressureThreshold`
 *           messages in their mailbox. Note that estimating mailbox size of
 *           default UnboundedMailbox is O(N) operation.</li>
 * </ul>
 */
  pressureThreshold: Int = 1,
  /**
 * Percentage to increase capacity whenever all routees are busy.
 * For example, 0.2 would increase 20% (rounded up), i.e. if current
 * capacity is 6 it will request an increase of 2 more routees.
 */
  rampupRate: Double = 0.2,
  /**
 * Minimum fraction of busy routees before backing off.
 * For example, if this is 0.3, then we'll remove some routees only when
 * less than 30% of routees are busy, i.e. if current capacity is 10 and
 * 3 are busy then the capacity is unchanged, but if 2 or less are busy
 * the capacity is decreased.
 *
 * Use 0.0 or negative to avoid removal of routees.
 */
  backoffThreshold: Double = 0.3,
  /**
 * Fraction of routees to be removed when the resizer reaches the
 * backoffThreshold.
 * For example, 0.1 would decrease 10% (rounded up), i.e. if current
 * capacity is 9 it will request an decrease of 1 routee.
 */
  backoffRate: Double = 0.1,
  /**
 * When the resizer reduce the capacity the abandoned routee actors are stopped
 * with PoisonPill after this delay. The reason for the delay is to give concurrent
 * messages a chance to be placed in mailbox before sending PoisonPill.
 * Use 0 seconds to skip delay.
 */
  // FIXME #3549
  stopDelay: FiniteDuration = 1.second,
  /**
 * Number of messages between resize operation.
 * Use 1 to resize before each message.
 */
  messagesPerResize: Int = 10) extends Resizer {

  /**
   * Java API constructor for default values except bounds.
   */
  def this(lower: Int, upper: Int) = this(lowerBound = lower, upperBound = upper)

  if (lowerBound < 0) throw new IllegalArgumentException("lowerBound must be >= 0, was: [%s]".format(lowerBound))
  if (upperBound < 0) throw new IllegalArgumentException("upperBound must be >= 0, was: [%s]".format(upperBound))
  if (upperBound < lowerBound) throw new IllegalArgumentException("upperBound must be >= lowerBound, was: [%s] < [%s]".format(upperBound, lowerBound))
  if (rampupRate < 0.0) throw new IllegalArgumentException("rampupRate must be >= 0.0, was [%s]".format(rampupRate))
  if (backoffThreshold > 1.0) throw new IllegalArgumentException("backoffThreshold must be <= 1.0, was [%s]".format(backoffThreshold))
  if (backoffRate < 0.0) throw new IllegalArgumentException("backoffRate must be >= 0.0, was [%s]".format(backoffRate))
  if (messagesPerResize <= 0) throw new IllegalArgumentException("messagesPerResize must be > 0, was [%s]".format(messagesPerResize))

  def isTimeForResize(messageCounter: Long): Boolean = (messageCounter % messagesPerResize == 0)

  override def resize(currentRoutees: immutable.IndexedSeq[Routee]): Int =
    capacity(currentRoutees)

  /**
   * Returns the overall desired change in resizer capacity. Positive value will
   * add routees to the resizer. Negative value will remove routees from the
   * resizer.
   * @param routees The current actor in the resizer
   * @return the number of routees by which the resizer should be adjusted (positive, negative or zero)
   */
  def capacity(routees: immutable.IndexedSeq[Routee]): Int = {
    val currentSize = routees.size
    val press = pressure(routees)
    val delta = filter(press, currentSize)
    val proposed = currentSize + delta

    if (proposed < lowerBound) delta + (lowerBound - proposed)
    else if (proposed > upperBound) delta - (proposed - upperBound)
    else delta
  }

  /**
   * Number of routees considered busy, or above 'pressure level'.
   *
   * Implementation depends on the value of `pressureThreshold`
   * (default is 1).
   * <ul>
   * <li> 0:   number of routees currently processing a message.</li>
   * <li> 1:   number of routees currently processing a message has
   *           some messages in mailbox.</li>
   * <li> > 1: number of routees with at least the configured `pressureThreshold`
   *           messages in their mailbox. Note that estimating mailbox size of
   *           default UnboundedMailbox is O(N) operation.</li>
   * </ul>
   *
   * @param routees the current resizer of routees
   * @return number of busy routees, between 0 and routees.size
   */
  def pressure(routees: immutable.IndexedSeq[Routee]): Int = {
    routees count {
      case ActorRefRoutee(a: ActorRefWithCell) ⇒
        a.underlying match {
          case cell: ActorCell ⇒
            pressureThreshold match {
              case 1          ⇒ cell.mailbox.isScheduled && cell.mailbox.hasMessages
              case i if i < 1 ⇒ cell.mailbox.isScheduled && cell.currentMessage != null
              case threshold  ⇒ cell.mailbox.numberOfMessages >= threshold
            }
          case cell ⇒
            pressureThreshold match {
              case 1          ⇒ cell.hasMessages
              case i if i < 1 ⇒ true // unstarted cells are always busy, for example
              case threshold  ⇒ cell.numberOfMessages >= threshold
            }
        }
      case x ⇒
        false
    }
  }

  /**
   * This method can be used to smooth the capacity delta by considering
   * the current pressure and current capacity.
   *
   * @param pressure current number of busy routees
   * @param capacity current number of routees
   * @return proposed change in the capacity
   */
  def filter(pressure: Int, capacity: Int): Int = rampup(pressure, capacity) + backoff(pressure, capacity)

  /**
   * Computes a proposed positive (or zero) capacity delta using
   * the configured `rampupRate`.
   * @param pressure the current number of busy routees
   * @param capacity the current number of total routees
   * @return proposed increase in capacity
   */
  def rampup(pressure: Int, capacity: Int): Int =
    if (pressure < capacity) 0 else math.ceil(rampupRate * capacity).toInt

  /**
   * Computes a proposed negative (or zero) capacity delta using
   * the configured `backoffThreshold` and `backoffRate`
   * @param pressure the current number of busy routees
   * @param capacity the current number of total routees
   * @return proposed decrease in capacity (as a negative number)
   */
  def backoff(pressure: Int, capacity: Int): Int =
    if (backoffThreshold > 0.0 && backoffRate > 0.0 && capacity > 0 && pressure.toDouble / capacity < backoffThreshold)
      math.floor(-1.0 * backoffRate * capacity).toInt
    else 0

}

trait Resizable extends RouterConfig2 { this: CreateChildRoutee ⇒
  // FIXME #3549 signature clash with old resizer method
  def resizer2: Option[Resizer]

  override def createActor(): Actor =
    resizer2 match {
      case Some(r) ⇒
        new RouterActorWithResizer {
          override def supervisorStrategy: SupervisorStrategy = super.supervisorStrategy
        }
      case _ ⇒ super.createActor()
    }

}

/**
 * INTERNAL API
 */
private[akka] final class ResizableRoutedActorCell(
  _system: ActorSystemImpl,
  _ref: InternalActorRef,
  _routerProps: Props,
  _routerDispatcher: MessageDispatcher,
  _routeeProps: Props,
  _supervisor: InternalActorRef,
  val resizer: Resizer)
  extends RoutedActorCell(_system, _ref, _routerProps, _routerDispatcher, _routeeProps, _supervisor) {

  private val childFactory = _routerProps.routerConfig.asInstanceOf[CreateChildRoutee]
  private val resizeInProgress = new AtomicBoolean
  private val resizeCounter = new AtomicLong

  override protected def preSuperStart(): Unit = {
    // initial resize, before message send
    if (resizer.isTimeForResize(resizeCounter.getAndIncrement())) {
      resize(initial = true)
    }
  }

  override def sendMessage(envelope: Envelope): Unit = {
    if (!targetSelf(envelope.message) &&
      resizer.isTimeForResize(resizeCounter.getAndIncrement()) && resizeInProgress.compareAndSet(false, true)) {
      super.sendMessage(Envelope(RouterActorWithResizer.Resize, self, system))
    }
    super.sendMessage(envelope)
  }

  println("# Resizer active")

  private[akka] def resize(initial: Boolean): Unit = {
    if (resizeInProgress.get || initial) try {
      val requestedCapacity = resizer.resize(router.routees)
      println("# resize: " + resizeCounter.get + " => " + router.routees.size + " + " + requestedCapacity)
      if (requestedCapacity > 0) {
        val newRoutees = immutable.IndexedSeq.fill(requestedCapacity)(childFactory.newRoutee(routeeProps, this))
        // FIXME #3549 watch newRoutees
        _router = _router.withRoutees(_router.routees ++ newRoutees)
      } else if (requestedCapacity < 0) {
        val currentRoutees = _router.routees
        val abandon = currentRoutees.drop(currentRoutees.length + requestedCapacity)

        delayedStop(abandon)
        // FIXME #3549 unwatch removed routees
        val newRoutees = abandon.foldLeft(currentRoutees) { (xs, x) ⇒ /*unwatch(x);*/ xs.filterNot(_ == x) }
        _router = _router.withRoutees(newRoutees)
      }
    } finally resizeInProgress.set(false)
  }

  /**
   * Give concurrent messages a chance to be placed in mailbox before
   * sending PoisonPill.
   */
  private def delayedStop(abandon: immutable.IndexedSeq[Routee]): Unit = {
    val stopDelay: FiniteDuration = 1.second // FIXME #3549 pick from config
    if (abandon.nonEmpty) {
      if (stopDelay <= Duration.Zero) {
        abandon.foreach(_.send(PoisonPill, self))
      } else {
        system.scheduler.scheduleOnce(stopDelay) {
          abandon.foreach(_.send(PoisonPill, self))
        }(dispatcher)
      }
    }
  }

}

/**
 * INTERNAL API
 */
private[akka] object RouterActorWithResizer {
  case object Resize extends RouterManagementMesssage
}

/**
 * INTERNAL API
 */
private[akka] class RouterActorWithResizer extends RouterActor {
  import RouterActorWithResizer._

  val resizerCell = context match {
    case x: ResizableRoutedActorCell ⇒ x
    case _ ⇒
      throw ActorInitializationException("Router actor can only be used in RoutedActorRef, not in " + context.getClass)
  }

  override def receive = ({
    case Resize ⇒ resizerCell.resize(initial = false)
  }: Actor.Receive) orElse super.receive
}
