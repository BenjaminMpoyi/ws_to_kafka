package com.pkinsky

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.stream._
import akka.stream.scaladsl._
import com.softwaremill.react.kafka._
import org.apache.kafka.common.serialization._
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext


object StreamingUpload extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val materializer: Materializer = ActorMaterializer()

  val kafkaClient: ReactiveKafka = new ReactiveKafka()

  val localKafka = "192.168.99.100:9092"

  val kafkaSink: Sink[Event, Unit] =
    Flow[Event].map(e => ProducerMessage(e)).to(
    Sink.fromSubscriber(
    kafkaClient.publish(
      ProducerProperties(
        bootstrapServers = localKafka, //IP and port of local Kafka instance
        topic = "events", // topic to publish message to
        valueSerializer = Event.serializer
      )
    )))

  val kafkaPublisherGraph: RunnableGraph[SourceQueue[Event]] =
    Source.queue[Event](1024, OverflowStrategy.backpressure)
      .to(kafkaSink)

  val sourceQueue: SourceQueue[Event] = kafkaPublisherGraph.run

  def queueWriter[T](queue: SourceQueue[T]): Sink[T, Unit] =
    Flow[T]
      .mapAsync(1)( elem => queue.offer(elem).map( notDropped => (notDropped, elem) ) )
      .to(Sink.foreach{
        case (false, elem) => println(s"error: elem $elem rejected by queue")
        case (true, elem) =>
      })

  val parseMessage: Flow[Message, Event, Unit] =
    Flow[Message].collect{
      case TextMessage.Strict(t) =>
        val js = Json.parse(t)
        Json.fromJson[Event](js).get
    }

  val flow: Flow[Message, Message, Unit] =
    Flow.fromSinkAndSource(
      sink = parseMessage.to(queueWriter(sourceQueue)),
      source = Source.maybe[Message]
    )

  val routes: Flow[HttpRequest, HttpResponse, Unit] =
    get {
      path(PathEnd) {
        println("/ executed")
        getFromResource("test.html")
      } ~
        path("ws") {
          println("/ws executed")
          handleWebsocketMessages(flow)
        }
    }

  Http().bindAndHandle(routes, "localhost", 9000).onComplete(println)

  val kafkaConsumer: Source[Event, Unit] =
    Source.fromPublisher(kafkaClient.consume(
      ConsumerProperties(
        bootstrapServers = localKafka, // IP and port of local Kafka instance
        topic = "events", // topic to consume messages from
        groupId = "group1", // consumer group
        valueDeserializer = Event.deserializer
      )
    )).map(_.value())

  kafkaConsumer.runForeach{ s => println(s"from kafka: $s")}
}