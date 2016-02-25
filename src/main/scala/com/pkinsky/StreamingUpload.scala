package com.pkinsky

import java.util.concurrent.atomic.AtomicInteger
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.stream._
import akka.stream.scaladsl._
import com.softwaremill.react.kafka._
import play.api.libs.json.Json
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

object StreamingUpload extends App with AppContext{

  val kafkaSink: Sink[Event, Unit] =
    Flow[Event].map(e => ProducerMessage(e)).to(
    Sink.fromSubscriber(
    kafkaClient.publish(
      ProducerProperties(
        bootstrapServers = localKafka, //IP and port of local Kafka instance
        topic = eventTopic, // topic to publish message to
        valueSerializer = JsonHelper.serializer[Event]
      )
    )))

  val kafkaPublisherGraph: RunnableGraph[SourceQueue[Event]] =
    Source.queue[Event](4096, OverflowStrategy.backpressure)
      .to(kafkaSink)

  val sourceQueue: SourceQueue[Event] = kafkaPublisherGraph.run

  val msgsReceived = new AtomicInteger(0)

  def queueWriter[T](queue: SourceQueue[T]): Sink[T, Unit] =
    Flow[T]
      .mapAsync(1){ elem => msgsReceived.incrementAndGet(); queue.offer(elem).map( notDropped => (notDropped, elem) ) }
      .to(Sink.foreach{
        case (false, elem) => println(s"error: elem $elem rejected by queue")
        case (true, elem) => //println(s"elem $elem accepted by queue")
      })

  val parseMessage: Flow[Message, Event, Unit] =
    Flow[Message]
    .collect{
      case TextMessage.Strict(t) =>
        val js = Json.parse(t)
        Json.fromJson[Event](js).get
    }

  def flow: Flow[Message, Message, Unit] = {
    Flow.fromSinkAndSource(
      sink = parseMessage.to(queueWriter(sourceQueue)),
      source = Source.maybe
    )
  }

  val routes: Flow[HttpRequest, HttpResponse, Unit] =
    get {
      path(PathEnd) {
        getFromResource("test.html")
      } ~
        path("ws") {
          handleWebsocketMessages(flow)
        }
    }

  Http().bindAndHandle(routes, "localhost", 9000).onComplete(println)

  System.console().readLine() //wait for enter

  println(s"received ${msgsReceived.get()} msgs, shutting down because enter was pressed")
  system.shutdown()
  System.exit(0)
}


