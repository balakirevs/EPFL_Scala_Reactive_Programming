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

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply
  
  case object CheckPersisted
  case class OperationTimeout(id: Long)
  case class OperationRetry(id: Long)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  import scala.language.postfixOps
  import Coordinator._
 
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // a map from a KEY to a Coordinator actor for that key
  var coordinators = Map.empty[String, ActorRef]
  // expected NEXT snapshot identifier
  private var expectedSnapshot = 0
  
  val persistence = context.actorOf(persistenceProps)
  
  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }
 
 
  /* TODO Behavior for  the leader role. */
  val leader: Receive = handleGetOperation orElse {
    case Insert(key, value, id) =>
      kv = kv + (key -> value)
      leaderCoordinator(key) ! Modification(sender, id, key, Some(value))
      
    case Remove(key, id) =>
      kv = kv - key
      leaderCoordinator(key) ! Modification(sender, id, key, None)
      
    case Replicas(replicas) =>
      onReplicasChange(replicas)
    
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = handleGetOperation orElse {

    case snapshot@Snapshot(key, valOption, seq) =>
      if (expectedSnapshot == seq) {
        kv = valOption.fold(kv - key)(kv.updated(key, _))
        expectedSnapshot += 1
      }
      if(seq < expectedSnapshot) replicaCoordinator(key) ! Modification(sender, seq, key, valOption)
  }
  
  override def preStart() {
    arbiter ! Join
  }
  
  private def handleGetOperation: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
  }
  
  //--
  // Helper Functions
  // --

  // Coordinator stuff
  private def coordinator(key: String)(ackBuilder: (Long, String) => Any) = coordinators.get(key).getOrElse {
    val coordinator = context.actorOf(Props(classOf[Coordinator], persistence, secondaries.values.toSet, ackBuilder))
    coordinators += (key -> coordinator)
    coordinator
  }

  private def leaderCoordinator(key: String) = coordinator(key)((id: Long, key: String) => OperationAck(id))

  private def replicaCoordinator(key: String) = coordinator(key)((seq: Long, key: String) => SnapshotAck(key, seq))

  // replicas lifecycle stuff
  private def onReplicasChange(replicas: Set[ActorRef]) =  {
    def isGone(replica: ActorRef) = secondaries.contains(replica) && !replicas.contains(replica)
    def isNew(replica: ActorRef)  = !secondaries.contains(replica) && replicas.contains(replica)
    
    def handleReplicaGone(replica: ActorRef) = {
      coordinators.values.foreach(_ ! Gone(secondaries(replica)))
      context.stop(secondaries(replica))
      secondaries -= replica
    }

    def handleReplicaAppears(replica: ActorRef) = {
      val replicator = context.actorOf(Props(classOf[Replicator], replica))
      coordinators.values.foreach(_ ! Entered(replicator))
      secondaries += (replica -> replicator)
      
      kv.foldLeft(0) { case (seq, (key, value)) =>
        replicator ! Replicate(key, Some(value), seq)
        seq - 1
      }
    }

    for (replica <- (replicas - self) ++ secondaries.keys.toSet) {
      if (isGone(replica)) handleReplicaGone(replica)
      else if (isNew(replica)) handleReplicaAppears(replica)
    }

  }

}
