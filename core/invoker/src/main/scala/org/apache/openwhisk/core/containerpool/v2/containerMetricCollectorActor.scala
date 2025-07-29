package org.apache.openwhisk.core.containerpool.v2

import scala.concurrent.duration._
import akka.actor.{Actor, Props}
import spray.json._
import java.time.Instant
import java.nio.file.{Files, Paths, StandardOpenOption}
import org.apache.openwhisk.core.containerpool.v2.ContainerMetricJsonProtocol._

/*
    Actor that collects container metrics and writes them to a file.
*/

// == Messages ==
case class ContainerLifeCycleEvent(
    containerId: String,
    invokerName: Option[String],
    actionName: Option[String],
    memReservedMB: Option[Int],
    created: Option[Instant],
    startInit: Option[Instant],
    endInit: Option[Instant],
    startRun: Seq[Instant],
    endRun: Seq[Instant],
    destroyed: Option[Instant]
)


// == Actor ==
class ContainerMetricCollectorActor(filePath: String) extends Actor {
    import context.dispatcher

    val path = Paths.get(filePath)
    Files.createDirectories(path.getParent)

    var metrics = Map[String, ContainerLifeCycleEvent]()
    val dumpTask = context.system.scheduler.scheduleWithFixedDelay(1.minute, 1.minute, self, "dump")

    override def postStop(): Unit = dumpTask.cancel()

    override def receive: Receive = {
        case report: ContainerLifeCycleEvent =>
            metrics += (report.containerId -> report)

            if (report.destroyed.isDefined) {
                Console.println(report.toJson.compactPrint)
                try {
                    Files.write(path, (report.toJson.compactPrint + "\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
                } catch {
                    case e: Exception =>
                        Console.println(s"Error writing to file: $e")
                }
                metrics -= report.containerId
            }

        case "dump" =>
            val eventsJson = metrics.values.map(_.toJson.compactPrint).mkString("\n")
            if (eventsJson.nonEmpty)
                Files.write(path, (eventsJson + "\n").getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
            metrics = Map.empty
    }
}

object ContainerMetricCollectorActor {
    def props(filePath: String): Props = Props(new ContainerMetricCollectorActor(filePath))
}