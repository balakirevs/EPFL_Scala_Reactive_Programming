package nodescala

import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.async.Async.{async, await}

object Main {

  def main(args: Array[String]) {
    def afterDelay[T](dur: Duration, value: T): Future[T] = {
      val p = Promise[T]
      
      Future.delay(dur) onComplete { case _ => p.success(value) }
      
      p.future
    }
    
    // TO IMPLEMENT
    // 1. instantiate the server at 8191, relative path "/test",
    //    and have the response return headers of the request
    val myServer = new NodeScala.Default(8191)
    val myServerSubscription = myServer.createListener("/test").start

    // TO IMPLEMENT
    // 2. create a future that expects some user input `x`
    //    and continues with a `"You entered... " + x` message
    val userInterrupted: Future[String] =
      Future.userInput("").continue(_ match {
        case Success(s) => "You entered ..." + s
        case Failure(e) => throw e
      })

    // TO IMPLEMENT
    // 3. create a future that completes after 20 seconds
    //    and continues with a `"Server timeout!"` message
    val timeOut: Future[String] =
      afterDelay(20 seconds, "Server timeout!")

    // TO IMPLEMENT
    // 4. create a future that completes when either 10 seconds elapse
    //    or the user enters some text and presses ENTER
    val terminationRequested: Future[String] = {
      val p = Promise[String]
      
      Future any List(userInterrupted, afterDelay(10 seconds, "Server closed!")) onComplete { p.complete(_) }
      
      p.future
    }
    

    // TO IMPLEMENT
    // 5. unsubscribe from the server
    terminationRequested onSuccess {
      case msg => myServerSubscription.unsubscribe
    }
  }

}