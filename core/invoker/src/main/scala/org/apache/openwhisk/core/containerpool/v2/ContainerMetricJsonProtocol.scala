package org.apache.openwhisk.core.containerpool.v2

import spray.json._
import java.time.Instant

/*
    A custom JSON protocol for the ContainerLifecycleEvent class.
    It is used to serialize and deserialize the ContainerLifecycleEvent class to and from JSON.
*/

object ContainerMetricJsonProtocol extends DefaultJsonProtocol {
    implicit val instantFormat = new JsonFormat[Instant] {
        override def write(i: Instant): JsValue = JsString(i.toString)
        override def read(json: JsValue): Instant = json match {
            case JsString(s) => Instant.parse(s)
            case _ => throw new DeserializationException("Expected Instant as JsString")
        }
    } 

    implicit val eventFormat: RootJsonFormat[ContainerLifeCycleEvent] = jsonFormat10(ContainerLifeCycleEvent)
}