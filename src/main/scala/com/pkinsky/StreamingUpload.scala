package com.pkinsky

import java.net.URI
import java.util.UUID

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.stream._
import akka.stream.scaladsl._
import com.softwaremill.react.kafka._
import play.api.libs.json.Json

import scala.concurrent.{Future, ExecutionContext}

import scala.concurrent.duration._
import scala.util.Random


object Topics {
  val msgTopic = "msg_topic_c"
  val eventTopic = "event_topic_c"
}



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
        topic = Topics.eventTopic, // topic to publish message to
        valueSerializer = Event.serializer
      )
    )))

  val kafkaPublisherGraph: RunnableGraph[SourceQueue[Event]] =
    Source.queue[Event](4096, OverflowStrategy.backpressure)
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
    Flow[Message]//.map{x => println("preparse: " + x); x}
    .collect{
      case TextMessage.Strict(t) =>
        val js = Json.parse(t)
        Json.fromJson[Event](js).get
    }

  def flow(id: Long): Flow[Message, Message, Unit] = {
    Flow.fromSinkAndSource(
      sink = parseMessage.to(queueWriter(sourceQueue)),
      source = Source.maybe
    )
  }

  //note: can just do this and skip routes nonsense
  //val testRoute: Flow[HttpRequest, HttpResponse, Unit] = handleWebsocketMessages(flow(id = ???))

  val routes: Flow[HttpRequest, HttpResponse, Unit] =
    get {
      path(PathEnd) {
        getFromResource("test.html")
      } ~
        path(IntNumber / "ws") { id =>
          handleWebsocketMessages(flow(id))
        }
    }

  Http().bindAndHandle(routes, "localhost", 9000).onComplete(println)
}


object Test extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val materializer: Materializer = ActorMaterializer()

  val kafkaClient: ReactiveKafka = new ReactiveKafka()

  val localKafka = "192.168.99.100:9092"

  def ws(id: Int) = Http().websocketClientFlow(WebsocketRequest(Uri(s"ws://localhost:9000/$id/ws"))).mapMaterializedValue{ f => f.onComplete{case x => println(s"ws client connection res $x")} }

  val userIds = 1 to 50

  val msgs = Random.shuffle(userIds.flatMap( id => (1 to 1000).map( n => Msg(id, s"msg #$n for id $id")) ))

  def sendMsgs(): Unit = {
    val kafkaSink: Sink[Msg, Unit] =
      Flow[Msg].map(e => ProducerMessage(e)).to(
        Sink.fromSubscriber(
          kafkaClient.publish(
            ProducerProperties(
              bootstrapServers = localKafka, //IP and port of local Kafka instance
              topic = Topics.msgTopic, // topic to publish message to
              valueSerializer = Msg.serializer
            )
          )))


    Source(msgs).throttle(500, 100 milliseconds, 1, ThrottleMode.Shaping).runWith(kafkaSink)


  }


  def sendAll = {
    val start = System.currentTimeMillis()

    Future.sequence(userIds.map{ n =>

      val parseMessage: Flow[Message, Msg, Unit] =
        Flow[Message].map{x => println("preparse msg: " + x); x}
          .collect{
          case TextMessage.Strict(t) =>
            val js = Json.parse(t)
            Json.fromJson[Msg](js).get
        }

      def data = Event(s"unique fake client id ${UUID.randomUUID()}", System.currentTimeMillis())

      val allData = Vector.fill(1000)(data)

      Source(allData).throttle(1, 100 millis, 1, ThrottleMode.Shaping)
        .map(e => Json.toJson(e))
        .map(j => TextMessage(j.toString()))
        .via(ws(n))
        .via(parseMessage)
        .toMat(Sink.fold(Set.empty[Msg])(_ ++ Set(_)))(Keep.right)
        .run().map( msgs => (allData, msgs))

    }).map{ vs =>
      val (evs, msgs) = vs.unzip
      (evs.flatten.toSet, msgs.flatten.toSet)
    }.andThen{
      case util.Success(_) => println(s"sendall completes after ${System.currentTimeMillis() - start} millis")
      case util.Failure(t) => println(s"failed: $t"); t.printStackTrace()
    }
  }

  def eventsK = {

    println("grabbing events from kafka")
    val kafkaConsumer: Source[Event, Unit] =
      Source.fromPublisher(kafkaClient.consume(
        ConsumerProperties(
          bootstrapServers = localKafka, // IP and port of local Kafka instance
          topic = Topics.eventTopic, // topic to consume messages from
          groupId = "group_new", // consumer group
          valueDeserializer = Event.deserializer
        )
      )).map(_.value()).takeWithin(1 minute)

    kafkaConsumer.runFold(Set.empty[Event])(_ ++ Set(_))
  }


  //start sending msgs to be sent back down kafka
  sendMsgs()
  val f = for {
      (sentEvents, recdMsgs) <- sendAll
      eventsK <- eventsK
    } yield(sentEvents, eventsK, recdMsgs)




  f.onComplete{
    case util.Success((events, events2, recdMsgs)) =>


      println(s"sent set: ${events.size}")
      println(s"kafka set: ${events2.size}")


      println(s"recd msgs: ${recdMsgs.size}")
      println(s"send msgs: ${msgs.toSet.size}")

    case util.Failure(t) => println(s"failed with $t")
  }

}

