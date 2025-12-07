/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool

import akka.actor.{Actor, ActorRef, Props, Terminated}
import org.apache.openwhisk.common.AkkaLogging
import org.apache.openwhisk.core.entity.size._
import spray.json._
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Route
import akka.util.ByteString
import akka.stream.SystemMaterializer


import scala.util.{Failure, Success}

object ProbingAgentBridgeProtocol extends DefaultJsonProtocol {
  final case class AgentUpdate(
    containerId: String,
    newLimitBytes: Long
  )
  implicit val agentUpdateFormat: RootJsonFormat[AgentUpdate] =
    jsonFormat2(AgentUpdate)
}

/**
 * Bridge Actor that manages communication between ContainerProxy and a single native ProbingAgent process.
 * 
 * Architecture:
 * ContainerPool
 *   ├── ContainerProxy (multiple)
 *   └── ProbingAgentBridge (single, shared) <-- HTTP push endpoint for commits
 *         └── Native ProbingAgent (single process, monitors multiple containers in batch)
 * 
 * Responsibilities:
 * - Spawn/manage a single native probing agent process
 * - Listen for HTTP requests to push commits
 * - Forward StartProbing/StopProbing messages to agent (add/remove containers from batch)
 */
class ProbingAgentBridge(agentAddress: String,
                         agentHttpPort: Int,
                         listeningHttpPort: Int = 50051)
    extends Actor {
  import ProbingAgentBridge._
  import ProbingAgentBridgeProtocol._

  implicit val logging = new AkkaLogging(context.system.log)
  implicit val materializer = SystemMaterializer(context.system).materializer
  implicit val ec = context.dispatcher
  implicit val system: akka.actor.ActorSystem = context.system

  // Map: containerId -> ContainerProxy ActorRef (for forwarding events)
  var activeProbes = Map.empty[String, ActorRef]
  private var pool: Option[ActorRef] = None
  
  // HTTP route: agent push endpoint for commits
  val route: Route = path("updateCommits") {
    post {
      entity(as[List[AgentUpdate]]) { updates =>
        if (updates.nonEmpty) {
          logging.debug(this, s"Received ${updates.size} commit updates")

          val poolUpdates = updates.map { u =>
            ContainerPool.ContainerMemoryDownsized(u.containerId, u.newLimitBytes.B)
          }

          pool match {
            case Some(p) => 
              p ! ContainerPool.CommitsUpdate(poolUpdates)
            case None =>
              logging.warn(this, "Pool not set, skipping commit updates")
          }
        }

        complete(StatusCodes.OK)
      }
    }
  }

  override def preStart(): Unit = {
    super.preStart()

    // http server binding
    Http().newServerAt(
      "0.0.0.0",
      listeningHttpPort
    ).bindFlow(route)
      .onComplete {
        case Success(binding) =>
          logging.debug(this, s"ProbingAgentBridge HTTP server started at http://0.0.0.0:$listeningHttpPort")
        case Failure(e) =>
          logging.error(this, s"Failed to start ProbingAgentBridge HTTP server: ${e.getMessage}")
      }
  }

  def receive: Receive = {
    case RegisterPool(poolref) =>
      pool = Some(poolref)
      logging.debug(this, "Pool registered")

    case StartProbing(containerId) =>
      val proxyRef = sender()
      addContainerToProbing(containerId, proxyRef)

    case StopProbing(containerId) =>
      removeContainerFromProbing(containerId)

    case UpdateProbing(containerId) =>
      sendUpdateProbingRequest(containerId)

    case Terminated(ref) =>
      // ContainerProxy terminated, remove from active probes
      activeProbes.find(_._2 == ref).foreach { case (containerId, _) =>
        removeContainerFromProbing(containerId)
        logging.debug(this, s"Container $containerId terminated, removed from active probes")
      }
  }

  private def addContainerToProbing(containerId: String,
                                    proxyRef: ActorRef): Unit = {
    if (activeProbes.contains(containerId)) {
      logging.warn(this, s"Probing already active for container $containerId")
      return
    }

    // Watch the proxy to clean up on termination
    context.watch(proxyRef)

    // Add container to active probes
    activeProbes = activeProbes + (containerId -> proxyRef)

    // Send add container request to agent (via HTTP or gRPC)
    sendAddContainerRequest(containerId)

    // TODO. calculate probetime dynamically
    // calculateProbeTime(containerId)

    logging.debug(this, s"Added container $containerId to probing batch")
  }

  private def removeContainerFromProbing(containerId: String): Unit = {
    activeProbes.get(containerId).foreach { proxyRef =>
      // Send remove container request to agent
      sendRemoveContainerRequest(containerId)
      
      context.unwatch(proxyRef)
      activeProbes = activeProbes - containerId
      logging.debug(this, s"Removed container $containerId from probing batch")
    }
  }


  private def sendAddContainerRequest(containerId: String): Unit = {
    // TODO: Implement HTTP call to agent to add container to monitoring batch
    // For now, this is a placeholder - actual implementation depends on agent API
    // Example HTTP call:
    // POST http://localhost:${agentHttpPort}/containers/add
    // Body: { "container_id": containerId }

    val json: JsValue = JsObject(
      "container_id" -> JsString(containerId),
      "probe_time" -> JsNumber(calculateProbeTime(containerId))
    )

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = s"http://$agentAddress:$agentHttpPort/containers/add",
      entity = HttpEntity(ContentTypes.`application/json`, ByteString(json.compactPrint))
    )

    Http(context.system)
      .singleRequest(request)
      .onComplete {
        case scala.util.Success(response: HttpResponse) if response.status.isSuccess() =>
          logging.debug(this, s"Successfully added container $containerId to agent batch")
        case scala.util.Success(response: HttpResponse) =>
          logging.warn(this, s"Failed to add container $containerId to agent batch: ${response.status}")
        case scala.util.Failure(e) =>
          logging.error(this, s"Error adding container $containerId to agent batch: ${e.getMessage}")
      }
  }

  private def sendRemoveContainerRequest(containerId: String): Unit = {
    val json: JsValue = JsObject(
      "container_id" -> JsString(containerId)
    )

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = s"http://$agentAddress:$agentHttpPort/containers/remove",
      entity = HttpEntity(ContentTypes.`application/json`, ByteString(json.compactPrint))
    )

    Http(context.system)
      .singleRequest(request)
      .onComplete {
        case scala.util.Success(response: HttpResponse) if response.status.isSuccess() =>
          logging.debug(this, s"Successfully removed container $containerId from agent batch")
        case scala.util.Success(response: HttpResponse) =>
          logging.warn(this, s"Failed to remove container $containerId from agent batch: ${response.status}")
        case scala.util.Failure(e) =>
          logging.error(this, s"Error removing container $containerId from agent batch: ${e.getMessage}")
        }
  }


  // We "touch" the container to let the agent know invocation happened
  private def sendUpdateProbingRequest(containerId: String): Unit = {
    val json: JsValue = JsObject(
      "container_id" -> JsString(containerId)
    )
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = s"http://$agentAddress:$agentHttpPort/containers/update",
      entity = HttpEntity(ContentTypes.`application/json`, ByteString(json.compactPrint))
    )


    Http(context.system)
      .singleRequest(request)
      .onComplete {
        case scala.util.Success(response: HttpResponse) if response.status.isSuccess() =>
          logging.debug(this, s"Successfully updated probing for container $containerId")
        case scala.util.Success(response: HttpResponse) =>
          logging.warn(this, s"Failed to update probing for container $containerId: ${response.status}")
        case scala.util.Failure(e) =>
          logging.error(this, s"Error updating probing for container $containerId: ${e.getMessage}")
      }
  }

  // return probe time in seconds
  private def calculateProbeTime(containerId: String): Int = {
    // TODO: Implement logic to calculate probe time dynamically
    // For now, this is a placeholder - actual implementation depends on agent API

    return 60 * 5 // 5 minutes
  }
}

object ProbingAgentBridge {

    // messages: ContainerProxy -> bridge
    final case class StartProbing(containerId: String)
    final case class StopProbing(containerId: String)   
    final case class UpdateProbing(containerId: String)

    final case class RegisterPool(poolref: ActorRef)

    def props(agentAddress : String,
                agentHttpPort: Int,
                listeningHttpPort: Int
                ): Props =
        Props(new ProbingAgentBridge(agentAddress, agentHttpPort, listeningHttpPort))
}

