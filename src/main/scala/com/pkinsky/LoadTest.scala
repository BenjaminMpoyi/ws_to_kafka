package com.pkinsky

import java.util.concurrent.atomic.AtomicInteger

import akka.http.scaladsl.model.ws.{InvalidUpgradeResponse, WebsocketUpgradeResponse, WebsocketRequest, TextMessage}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Keep, Sink, RunnableGraph, Source}
import com.softwaremill.react.kafka.ConsumerProperties
import play.api.libs.json.Json

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.language.postfixOps

object LoadTest extends App with AppContext {
  val clients = 256
  val msgsPerClient = 1024

  val msgsSent = new AtomicInteger(0)

  def testData(clientId: String): Source[Event, Unit] =
    Source.unfoldInf(1) { n =>
      val event = Event(s"msg number $n", clientId, System.currentTimeMillis())
      (n + 1, event)
    }.take(msgsPerClient).throttle(1, 100 millis, 1, ThrottleMode.Shaping)

  def ws(clientId: String): RunnableGraph[Future[WebsocketUpgradeResponse]] =
    testData(clientId).map(e => TextMessage.Strict(Json.toJson(e).toString))
      .map{ x => msgsSent.incrementAndGet(); x}
      .viaMat(Http().websocketClientFlow(WebsocketRequest(Uri(s"ws://localhost:9000/ws"))))(Keep.right).to(Sink.ignore)


  Future.sequence((1 to clients).map{ id =>
    ws(s"client $id").run()
  }).map(_.collect{ case x: InvalidUpgradeResponse => x}).onComplete(println)


  def events: Source[Event, Unit] =
    Source.fromPublisher(kafkaClient.consume(
      ConsumerProperties(
        bootstrapServers = localKafka, // IP and port of local Kafka instance
        topic = eventTopic, // topic to consume messages from
        groupId = "group_new", // consumer group
        valueDeserializer = JsonHelper.deserializer[Event]
      )
    )).map(_.value()).take(clients * msgsPerClient).takeWithin(5 minutes)

  val fromKafkaF = events.runWith(Sink.seq)


  val res = Await.result(fromKafkaF, 6 minutes)
  println(s"sent ${msgsSent.get()} msgs total")
  println(s"res size: ${res.length}")
}