package com.pkinsky

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.softwaremill.react.kafka.ReactiveKafka
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.language.postfixOps


trait AppContext {
  //implicit context: actor system, execution context, materializer
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val mat: Materializer = ActorMaterializer()

  //kafka setup
  val kafkaConf = KafkaServiceConf("192.168.99.100:9092")
  val kafkaClient: ReactiveKafka = new ReactiveKafka()
  val kafka = new KafkaService(kafkaClient, kafkaConf)

  //constants
  val eventTopic = "event_topic_newer" //kafka topic
  val port = 9000 //server port

  def awaitTermination() = {
    System.console().readLine() //wait for enter

    println(s"shutting down because enter was pressed")
    system.shutdown()

    system.awaitTermination(30 seconds)
    System.exit(0)
  }
}