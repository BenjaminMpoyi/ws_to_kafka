package com.pkinsky

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.Json


case class Event(clientId: String, timestamp: Long)

object Event {
  implicit val format = Json.format[Event]

  val serializer = new Serializer[Event] {
    override def serialize(topic: String, data: Event): Array[Byte] = {
      val js = Json.toJson(data)
      js.toString().getBytes("UTF-8")
    }

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
    override def close(): Unit = ()
  }

  val deserializer = new Deserializer[Event] {
    override def deserialize(topic: String, data: Array[Byte]): Event = {
      val s = new String(data, "UTF-8")
      Json.fromJson(Json.parse(s)).get //throw exception on error
    }

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
    override def close(): Unit = ()
  }
}