package com.pkinsky

import java.util.concurrent.atomic.AtomicInteger

import akka.http.scaladsl.model.ws.{InvalidUpgradeResponse, WebsocketUpgradeResponse, WebsocketRequest, TextMessage}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Keep, Sink, RunnableGraph, Source}
import play.api.libs.json.Json

import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.language.postfixOps

object LoadTest extends App with AppContext {
  val clients = 256
  val eventsPerClient = 256

  val eventsSent = new AtomicInteger(0)

  def testData(clientId: String): Source[Event, Unit] =
    Source.unfoldInf(1) { n =>
      val event = Event(s"msg number $n", clientId, System.currentTimeMillis())
      (n + 1, event)
    }.take(eventsPerClient).throttle(1, 100 millis, 1, ThrottleMode.Shaping)

  def wsClient(clientId: String): RunnableGraph[Future[WebsocketUpgradeResponse]] =
    testData(clientId).map(e => TextMessage.Strict(Json.toJson(e).toString))
      .map { x => eventsSent.incrementAndGet(); x }
      .viaMat(Http().websocketClientFlow(WebsocketRequest(Uri(s"ws://localhost:$port/ws"))))(Keep.right).to(Sink.ignore)

  //set up websocket connections
  (1 to clients).foreach { id =>
    wsClient(s"client $id").run()
  }

  //watch kafka for messages sent via websocket
  val kafkaConsumerGraph: RunnableGraph[Future[Seq[Event]]] =
    kafka.consume[Event](eventTopic, "group_new")
      .take(clients * eventsPerClient).takeWithin(2 minutes)
      .toMat(Sink.seq)(Keep.right)

  val res = Await.result(kafkaConsumerGraph.run, 5 minutes)
  println(s"sent ${eventsSent.get()} events total")
  println(s"res size: ${res.length}")
}