package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  
  case class CheckAck(id: Long)
  case object FlushPending

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  import scala.language.postfixOps
  
  val flushScheduler = context.system.scheduler.schedule(100 milliseconds, 100 milliseconds, self, FlushPending)
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // map from sequence to ack tracker (scheduler)
  var ackTrackers = Map.empty[Long, akka.actor.Cancellable]
  
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    
    case r@Replicate(key, valueOption, id) =>
      val seq = nextSeq
      acks += (seq -> (sender, r))
      enqueueForBatchUpdate(Snapshot(key, valueOption, seq))
    
    case FlushPending =>
      pending.foreach(tellReplicaAndScheduleAckCheck)
      pending = Vector.empty[Snapshot]
    
    case SnapshotAck(key, seq) => 
      sendReplicated(seq)
    
    case CheckAck(seq) =>
      acks.get(seq).map { case (_, replicate) =>
        tellReplicaAndScheduleAckCheck(Snapshot(replicate.key, replicate.valueOption, seq))
      }
      
    case _ =>
  }
  
  override def postStop(): Unit = {
    flushScheduler.cancel()
    ackTrackers.values.foreach(_.cancel())
  }
  
  // --
  // Helper methods
  // --
  
  private def enqueueForBatchUpdate(snapshot: Snapshot): Unit = {
    val ix = pending.indexWhere(snapshot.key == _.key)
    if (ix != -1) dequeueExistingSnapshot(ix)
    pending :+= snapshot
  }
  
  private def dequeueExistingSnapshot(index: Int) = {
    sendReplicated(pending(index).seq)
    pending = pending.patch(index, Nil, 1)
  }
  
  private def tellReplicaAndScheduleAckCheck(snapshot: Snapshot): Unit = {
    replica ! snapshot
    ackTrackers += (snapshot.seq -> context.system.scheduler.scheduleOnce(150 milliseconds, self, CheckAck(snapshot.seq)))
  }
  
  private def sendReplicated(seq: Long) = for((op, replicate) <- acks.get(seq)) { 
    removeAckTracker(seq)
    acks = acks - seq
    op ! Replicated(replicate.key, replicate.id)
  }
  
  private def removeAckTracker(seq: Long) = ackTrackers.get(seq).map { tracker =>
    tracker.cancel()
    ackTrackers = ackTrackers - seq  
  }
  
}

