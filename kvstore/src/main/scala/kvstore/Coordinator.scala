package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Coordinator {
  import Replica._
    
  // represents modification (can be insertion or deletion). Requester is the one to which reply will be sent
  case class Modification(requester: ActorRef, id: Long, key: String, valueOption: Option[String])
  
  // message sent when the
  sealed trait ReplicatorLifecycle
  case class Gone(replicator: ActorRef) extends ReplicatorLifecycle
  case class Entered(replicator: ActorRef) extends ReplicatorLifecycle
  
  sealed trait SyncLifecycle {
    def m: Modification
  }
  case class SyncTimeout(val m: Modification) extends SyncLifecycle
  case class SyncSuccessful(val m: Modification) extends SyncLifecycle
  case class EnsurePersisted(val m: Modification) extends SyncLifecycle
  
}

/**
* Actor that coordinates updates of the keys accross all sources (persistence store and replicas). It handles timeouts and acks
* 
* Ack builder builds a response message, when given a message identifier and a key
*/
class Coordinator(var persistence: ActorRef, var replicators: Set[ActorRef], val ackBuilder: (Long, String) => Any) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  import akka.actor.Cancellable
  import scala.language.postfixOps
  import Coordinator._
  
  // when defined contains deadline time tracker (operation must finish within deadline)
  var deadlineTracker: Option[Cancellable] = None
  
  // when defined contains persistence tracker (resends persistence requests if persistence gets down)
  var persistenceTracker: Option[Cancellable] = None
  
  def receive = waiting
  
  def waiting: Receive = {
    case Gone(r) =>
      replicators -= r
    
    case Entered(r) =>
      replicators += r
    
    case m: Modification =>
      context.become(runNext(Queue(m)))
      
    case _ =>
  }
  
  def runNext(queue: Queue[Modification]): Receive = {
    deadlineTracker.map(_.cancel())
    persistenceTracker.map(_.cancel())
    
    if(queue.isEmpty) waiting
    else {
      val mod = queue.head
      persistence ! Persist(mod.key, mod.valueOption, mod.id)
      replicators.foreach(_ ! Replicate(mod.key, mod.valueOption, mod.id))
      
      deadlineTracker = Some(context.system.scheduler.scheduleOnce(1 second, self, SyncTimeout(mod)))
      persistenceTracker = Some(context.system.scheduler.scheduleOnce(100 milliseconds, self, EnsurePersisted(mod)))
      synchronize(queue, false, replicators)
    }
  }
  
  def synchronize(queue: Queue[Modification], persisted: Boolean, partners: Set[ActorRef]): Receive = {
    val mod = queue.head
    if (persisted && partners.isEmpty) {
      mod.requester ! ackBuilder(mod.id, mod.key)
      runNext(queue.tail)
    } else awaitSynchronization(queue, persisted, partners)
  }
  
  def awaitSynchronization(queue: Queue[Modification], persisted: Boolean, partners: Set[ActorRef]): Receive = {
  
    case EnsurePersisted(op) if op == queue.head =>
      persistence ! Persist(op.key, op.valueOption, op.id)
      persistenceTracker = Some(context.system.scheduler.scheduleOnce(100 milliseconds, self, EnsurePersisted(op)))
      
    case Persisted(key, id) if id == queue.head.id =>
      persistenceTracker.map(_.cancel())
      context.become(synchronize(queue, persisted = true, partners))
      
    case Replicated(key, id) if id == queue.head.id =>
      context.become(synchronize(queue, persisted, partners - sender))
      
    case SyncTimeout(op) if op == queue.head =>
      val mod = queue.head
      mod.requester ! OperationFailed(mod.id)
      context.become(runNext(queue.tail))
    
    case m: Modification =>
      context.become(awaitSynchronization(queue :+ m, persisted, partners))
      
    case Gone(r) =>
      replicators -= r
      context.become(synchronize(queue, persisted, partners - r))
    
    case Entered(r) =>
      replicators += r
      val mod = queue.head
      r ! Replicate(mod.key, mod.valueOption, mod.id)
      context.become(synchronize(queue, persisted, partners + r))

    case _ => ???
  }
  
  override def postStop(): Unit = {
    persistenceTracker.map(_.cancel()) 
    deadlineTracker.map(_.cancel())
  }
   
}
