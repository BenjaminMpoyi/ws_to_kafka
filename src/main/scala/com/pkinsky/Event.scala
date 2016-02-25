package com.pkinsky

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.{Reads, Writes, Json}


case class Event(msg: String, clientId: String, timestamp: Long)

object Event {
  implicit val format = Json.format[Event]
}

object JsonHelper{
  def serializer[T: Writes] = new Serializer[T] {
    override def serialize(topic: String, data: T): Array[Byte] = {
      val js = Json.toJson(data)
      js.toString().getBytes("UTF-8")
    }

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
    override def close(): Unit = ()
  }

  def deserializer[T: Reads] = new Deserializer[T] {
    override def deserialize(topic: String, data: Array[Byte]): T = {
      val s = new String(data, "UTF-8")
      Json.fromJson(Json.parse(s)).get //throw exception on error
    }

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
    override def close(): Unit = ()
  }
}
