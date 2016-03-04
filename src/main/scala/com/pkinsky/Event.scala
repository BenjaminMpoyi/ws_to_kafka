package com.pkinsky

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.{Reads, Writes, Json}

case class Event(msg: String, clientId: String, timestamp: Long)

object Event {
  implicit val format = Json.format[Event]
}
