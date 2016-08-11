package com.pkinsky


import akka.actor.ActorSystem
import akka.stream.scaladsl.{Source, Flow, Sink}
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerProperties, ProducerMessage, ReactiveKafka}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import play.api.libs.json.{Json, Reads, Writes}

case class KafkaServiceConf(bootstrapServers: String)

class KafkaService(kafkaClient: ReactiveKafka, conf: KafkaServiceConf) {
  /**
    * publish a stream of json-serializable messages to a kafka topic
    */
  def publish[T](topic: String)(implicit writes: Writes[T], actorSystem: ActorSystem): Sink[T, Unit] =
    Flow[T].map(e => ProducerMessage(e)).to(
      Sink.fromSubscriber(
        kafkaClient.publish(
          ProducerProperties(
            bootstrapServers = conf.bootstrapServers, //IP and port of local Kafka instance
            topic = topic, // topic to publish message to
            valueSerializer = KafkaService.serializer[T]
          )
        )))

  /**
    * consume messages from a kafka topic. messages must be deserializable from json
    */
  def consume[T](topic: String, groupId: String)(implicit writes: Reads[T], actorSystem: ActorSystem): Source[T, Unit] =
    Source.fromPublisher(kafkaClient.consume(
      ConsumerProperties(
        bootstrapServers = conf.bootstrapServers, // IP and port of local Kafka instance
        topic = topic, // topic to consume messages from
        groupId = groupId, // consumer group
        valueDeserializer = KafkaService.deserializer[T]
      )
    )).map(_.value())
}


object KafkaService {
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
      Json.fromJson(Json.parse(s)).get //throw exception on error ¯\_(ツ)_/¯ (consider returning JsResult[T])
    }

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
    override def close(): Unit = ()
  }
}
