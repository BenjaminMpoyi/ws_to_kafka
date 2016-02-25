package com.pkinsky

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.softwaremill.react.kafka.ReactiveKafka

import scala.concurrent.ExecutionContext


trait AppContext {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val materializer: Materializer = ActorMaterializer()

  val kafkaClient: ReactiveKafka = new ReactiveKafka()
  val localKafka = "192.168.99.100:9092"

  val eventTopic = "event_topic_newest1a23"
}