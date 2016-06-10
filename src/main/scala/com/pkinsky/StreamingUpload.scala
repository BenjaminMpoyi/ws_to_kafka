package com.pkinsky

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpResponse, HttpRequest}
import akka.http.scaladsl.model.ws.{TextMessage, Message}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.PathMatchers.PathEnd
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy, SourceQueue}
import akka.stream.scaladsl._
import com.softwaremill.react.kafka.ReactiveKafka
import play.api.libs.json.Json
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Success
import scala.language.postfixOps

object StreamingUpload extends App with AppContext {
  val kafkaPublisherGraph: RunnableGraph[SourceQueue[Event]] =
    Source.queue[Event](1024, OverflowStrategy.backpressure).to(kafka.publish[Event](eventTopic))

  val sourceQueue: SourceQueue[Event] = kafkaPublisherGraph.run

  val queueWriter: Sink[Event, Unit] =
    Flow[Event].mapAsync(1){ elem =>
      sourceQueue.offer(elem)
        .andThen{
          case Success(false) => println(s"failed to publish $elem to topic $eventTopic")
          case Success(true) => println(s"published $elem to topic $eventTopic")
        }
    }.to(Sink.ignore)

  val parseMessages: Flow[Message, Event, Unit] =
    Flow[Message]
      .collect{
        case TextMessage.Strict(t) =>
          val js = Json.parse(t)
          Json.fromJson[Event](js).get
      }

  val wsHandlerFlow: Flow[Message, Message, Unit] =
    Flow.fromSinkAndSource(
      sink = parseMessages.to(queueWriter),
      source = Source.maybe
    )

  val routes: Flow[HttpRequest, HttpResponse, Unit] =
      get {
        path(PathEnd) {
          getFromResource("test.html")
        } ~
          path("ws") {
            println("ws connection accepted")
            handleWebsocketMessages(wsHandlerFlow)
          }
      }

  Http().bindAndHandle(routes, "localhost", port).andThen{ case util.Failure(t) => println(s"error binding to localhost: $t")}

  println(s"listening on port $port, press ENTER to stop")

  awaitTermination()
}

object KafkaListener extends App with AppContext {
  val graph = kafka.consume[Event](eventTopic, "kafka_listener").toMat(Sink.foreach(println))(Keep.right)

  graph.run.onComplete(println)

  println(s"listening to Kafka topic $eventTopic, press ENTER to stop")

  awaitTermination()
}