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
  
  final case class AgentProbeDisabled(
    container_id: String,
    reason: String
  )
  implicit val agentProbeDisabledFormat: RootJsonFormat[AgentProbeDisabled] =
    jsonFormat2(AgentProbeDisabled.apply)
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
  val route: Route = 
    path("updateCommits") {
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
    } ~
    path("probeDisabled") {
      post {
        entity(as[AgentProbeDisabled]) { disabled =>
          // logging.info(this, s"Received probe disabled notification for container ${disabled.container_id}, reason: ${disabled.reason}")
          
          activeProbes.get(disabled.container_id).foreach { proxyRef =>
            proxyRef ! ProbeDisabled(disabled.reason)
            // logging.debug(this, s"Forwarded probe disabled message to ContainerProxy for ${disabled.container_id}")
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
          // logging.debug(this, s"ProbingAgentBridge HTTP server started at http://0.0.0.0:$listeningHttpPort")
        case Failure(e) =>
          logging.error(this, s"Failed to start ProbingAgentBridge HTTP server: ${e.getMessage}")
      }
    
    // Warm up HTTP connection to agent to avoid first-request delay
    // This establishes the connection pool early, reducing latency for the first actual request
    warmupAgentConnection()
  }
  
  private def warmupAgentConnection(): Unit = {
    // Try to establish connection early by sending a lightweight request
    // This pre-warms the connection pool and reduces first-request latency
    val healthRequest = HttpRequest(
      method = HttpMethods.GET,
      uri = s"http://$agentAddress:$agentHttpPort/health" // Try health endpoint first
    )
    
    Http(context.system)
      .singleRequest(healthRequest)
      .onComplete {
        case Success(response) =>
          response.discardEntityBytes()
          // logging.debug(this, s"Warmed up connection to agent at $agentAddress:$agentHttpPort via health endpoint")
        case Failure(_) =>
          // Health endpoint might not exist, try a lightweight request to containers endpoint
          // This still establishes the connection pool
          val dummyRequest = HttpRequest(
            method = HttpMethods.GET,
            uri = s"http://$agentAddress:$agentHttpPort/containers" // Might return 404, but establishes connection
          )
          Http(context.system)
            .singleRequest(dummyRequest)
            .onComplete {
              case Success(response) =>
                response.discardEntityBytes()
                // logging.debug(this, s"Warmed up connection to agent at $agentAddress:$agentHttpPort")
              case Failure(e) =>
                // Connection will be established on first real request
                // logging.debug(this, s"Connection warmup failed (will establish on first request): ${e.getMessage}")
            }
      }
  }

  def receive: Receive = {
    case RegisterPool(poolref) =>
      pool = Some(poolref)
      // logging.debug(this, "Pool registered")

    case StartProbing(containerId, stats) =>
      val proxyRef = sender()
      addContainerToProbing(containerId, proxyRef, stats)

    case StopProbing(containerId) =>
      removeContainerFromProbing(containerId)

    case UpdateProbing(containerId) =>
      sendUpdateProbingRequest(containerId)

    case Terminated(ref) =>
      // ContainerProxy terminated, remove from active probes
      activeProbes.find(_._2 == ref).foreach { case (containerId, _) =>
        removeContainerFromProbing(containerId)
        // logging.debug(this, s"Container $containerId terminated, removed from active probes")
      }
  }

  private def addContainerToProbing(containerId: String,
                                    proxyRef: ActorRef,
                                    stats: Option[ActionStats]): Unit = {
    if (activeProbes.contains(containerId)) {
      logging.warn(this, s"Probing already active for container $containerId")
      return
    }

    // Watch the proxy to clean up on termination
    context.watch(proxyRef)

    // Add container to active probes
    activeProbes = activeProbes + (containerId -> proxyRef)

    // Send add container request to agent (via HTTP or gRPC)
    sendAddContainerRequest(containerId, stats)

    // TODO. calculate probetime dynamically
    // calculateProbeTime(containerId)

    // logging.debug(this, s"Added container $containerId to probing batch")
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


  private def sendAddContainerRequest(containerId: String, stats: Option[ActionStats]): Unit = {
    // Build JSON request with ActionStats metrics for probing parameter adjustment
    var fields = Map(
      "container_id" -> JsString(containerId),
      "probe_time" -> JsNumber(calculateProbeTime(containerId))
    )
    
    // Add ActionStats metrics if available (only essential metrics)
    stats.foreach { s =>
      fields = fields ++ Map(
        "coldstart_sensitivity" -> JsNumber(s.coldstartSensitivity),
        "iat" -> JsNumber(s.iat),
        "cv" -> JsNumber(s.cv)
      )
      logging.debug(this, 
        s"Sending ActionStats for container $containerId: " +
        s"sensitivity=${s.coldstartSensitivity}, iat=${s.iat}s, cv=${s.cv}")
    }

    val json: JsValue = JsObject(fields)

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
    // Only send update request if container is actively being probed
    // This prevents sending requests for containers that have been removed
    if (!activeProbes.contains(containerId)) {
      logging.debug(this, s"Skipping update probing request for container $containerId (not in active probes)")
      return
    }
    
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
    final case class StartProbing(containerId: String, stats: Option[ActionStats] = None)
    final case class StopProbing(containerId: String)   
    final case class UpdateProbing(containerId: String)
    
    // messages: bridge -> ContainerProxy
    final case class ProbeDisabled(reason: String)

    final case class RegisterPool(poolref: ActorRef)

    def props(agentAddress : String,
                agentHttpPort: Int,
                listeningHttpPort: Int
                ): Props =
        Props(new ProbingAgentBridge(agentAddress, agentHttpPort, listeningHttpPort))
}

