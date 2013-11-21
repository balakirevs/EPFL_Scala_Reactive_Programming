package nodescala



import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be created") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }

  test("A Future should never be created") {
    val never = Future.never[Int]

    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }

  test("CancellationTokenSource should allow stopping the computation") {
    val cts = CancellationTokenSource()
    val ct = cts.cancellationToken
    val p = Promise[String]()

    async {
      while (ct.nonCancelled) {
        // do work
      }

      p.success("done")
    }

    cts.unsubscribe()
    assert(Await.result(p.future, 1 second) == "done")
  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }

  test("Listener should serve the next request as a future") {
    val dummy = new DummyListener(8191, "/test")
    val subscription = dummy.start()

    def test(req: Request) {
      val f = dummy.nextRequest()
      dummy.emit(req)
      val (reqReturned, xchg) = Await.result(f, 1 second)
      
      assert(reqReturned == req)
    }

    test(immutable.Map("StrangeHeader" -> List("StrangeValue1")))
    test(immutable.Map("StrangeHeader" -> List("StrangeValue2")))

    subscription.unsubscribe()
  }

  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)      
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

  
  /**********************\
  |* USER DEFINED TESTS *|
  \**********************/
  test("any fn test") {
    def afterDelay[T](d: Duration, t: T): Future[T] = {
      val p = Promise[T]
      
      Future.delay(d) onComplete { _ => p.success(t) }
      
      p.future
    }
    
    for (n <- 0 until 10)
    yield {
      val l = List.fill(20)(math.random).zipWithIndex
      
      val min = l.minBy(_._1)._2
      val any = Future.any(l.map(a => a match { case (dur, n) => afterDelay(dur seconds, n) }))
      
      any onSuccess { case a => assert(min === a) }
    }
  }
  
  test("now test") {
    val f1 = Future.always(5)
    Thread.sleep(1)
    assert(f1.now === 5)
    
    val f2 = Future.delay(10 milliseconds)
    assert(try   { f2.now; false                          }
           catch { case e: NoSuchElementException => true })
  }
}