package agent

import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext.Implicits.global
import com.github.nscala_time.time.Imports._

object AppMain extends App {
  
  val system = ActorSystem("sys")
    
  import scala.concurrent.Await
  import scala.concurrent.duration._
  XmlAgent.startup(system)
  XmlAgent.parseOutput
  Await.result(system.whenTerminated, Duration.Inf)
}