Server that consumes events via many websocket connections and publishes them to a Kafka topic
============================


Your app is a hit. Everyone's using your service. You've instrumented your website to collect real time usage data and you've set up a data processing pipeline to work with large amounts of data in real time, but how do you bridge the gap between web page and data pipeline? With websockets, Kafka and Akka Streams, of course!

This blog post will show you how to build and test a server that accepts websocket connections and publishes json-encoded events received via websocket to kafka as json-encoded strings. I'll be explaining things as I go, but some familiarity with Akka Streams will help: you should know how to create, combine and materialize (run) Sources, Sinks and Flows using Akka Streams.

(Note: the code is as solid as I can make it, but this blog post is a work in progress based on a talk I gave at NEScala 2016)

```scala
case class Event(msg: String, clientId: String, timestamp: Long)

object Event {
  implicit val format = Json.format[Event]
}
```

Events themselves are quite simple: an Event consists of a message, a client id and a timestamp. The play-json library is used to create an instance of the `Format` type class for `Event`, which provides the machinery required to convert Json objects to `Events` and vice versa. 

Working with Kafka
------------------

You'll need a Kafka 0.9 instance to follow along. If you use docker machine you can use the provided kafka_0.9.sh to get one running.

We'll be using the Reactive Kafka library to publish streams of messages to Kafka and consume streams of messages from Kafka. First, we'll need to create serializers and deserializers that the Kafka client can use to serialize and deserialize streams of messages. 

```scala
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
      Json.fromJson(Json.parse(s)).get //throw exception on error
    }

    override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()
    override def close(): Unit = ()
  }
}
```

Most of this is just boilerplate: `serializer[T: Writes]` constructs a Kafka `Serializer[T]` for any type that can be written to a Json object. `deserializer[T: Reads]` constructs a Kafka `Deserializer[T]` for any type that can be read from a Json object. Now that we have a way to serialize and deserialize `Events` (which, via the implicit `Format` type class instance created earlier can be read from and written to Json objects), let's build something to handle publishing and consuming streams of messages.

```scala
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
```

`KafkaService` provides a simplified interface via which we can publish streams of Json-encodable messages to Kafka and consume streams of Json-decodable messages from Kafka. `publish[T](topic)` creates a `Sink[T, Unit]` that consumes a stream of Json-encodable messages of type `T` and publishes them to the provided Kafka topic. `consume[T](topic, groupId)` creates a `Source[T, Unit]` that produces a stream of Json-decodable messages, read from the provided Kafka topic using the provided group id.

If you're not familiar with the specifics of Kafka, don't worry: we've just constructed a black box that abstracts away most of the complexity of Kakfa, allowing us to focus on the task at hand: transforming streams of messages.

Building Our App
----------------


First, we'll need to set up some context so we can run stream processing graphs, map over futures, et cetera. We'll use an `AppContext` trait to provide the required implicit context, some constants (the port used by our server and the topic to which events are published), and a pre-configured Kafka client.

```scala
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
}
```

First, we create a graph that, when materialized, returns a `SourceQueue[Event]` and run it using the implicit materializer to get `sourceQueue`. Now we can publish messages to Kafka using `sourceQueue.offer(event)`, which takes an element of type `Event` and returns a `Future[Boolean]` which completes with `true` if the element was added to the queue or `false` if it was dropped.

```scala
val kafkaPublisherGraph: RunnableGraph[SourceQueue[Event]] =
  Source.queue[Event](1024, OverflowStrategy.backpressure).to(kafka.publish[Event](eventTopic))

val sourceQueue: SourceQueue[Event] = kafkaPublisherGraph.run
```

Now that we have a running stream processing graph that publishes events to Kafka, let's create a Sink that publishes messages to the `SourceQueue`. We'll be materializing this Sink multiple times, once per websocket connection.

```
val queueWriter: Sink[Event, Unit] =
  Flow[Event].mapAsync(1){ elem =>
    sourceQueue.offer(elem)
      .andThen{
        case Success(false) => println(s"failed to publish $elem to topic $eventTopic")
      }
  }.to(Sink.ignore)
```


Working With Websockets
-----------------------

First, we'll need to parse incoming websocket `Messages`. We're only interested in `Strict` `TextMessages`, not streaming text messages or streaming or strict binary messages.

```scala
  val parseMessage: Flow[Message, Event, Unit] =
    Flow[Message]
      .collect{
        case TextMessage.Strict(t) =>
          val js = Json.parse(t)
          Json.fromJson[Event](js).get
      }
```

`parseMessages` uses `collect` to map over only `Strict` `TextMessages` using a partial function that attempts to parse the text payload of each websocket message as a Json-encoded event.

```
  val wsHandlerFlow: Flow[Message, Message, Unit] =
    Flow.fromSinkAndSource(
      sink = parseMessage.to(queueWriter),
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

  Http().bindAndHandle(routes, "localhost", port)

```

Running the Server
------------------

First, run the app using `sbt run` and choose the third option, which should look something like `[3] com.pkinsky.StreamingUpload`. Once the server is running, open 'localhost:9000' using your browser. The server should serve up a simple blank test page that logs events once per second. You can verify that messages are being sent using the developer console. Leave this page open.

To confirm that our events are making it to Kafka, let's create a simple listener that consumes messages from our event topic and prints them using println.

```scala
object KafkaListener extends App with AppContext {
  val graph = kafka.consume[Event](eventTopic, "kafka_listener").to(Sink.foreach(println))

  graph.run
}
```

Run this app by using `sbt run` and choosing the first option, which should look something like '[1] com.pkinsky.KafkaListener'.

You should see output that looks something like this:
 
```scala
> sbt run

Multiple main classes detected, select one to run:

 [1] com.pkinsky.KafkaListener
 [2] com.pkinsky.LoadTest
 [3] com.pkinsky.StreamingUpload

Enter number: 1

[info] Running com.pkinsky.KafkaListener
Event(test msg,0.272857979638502,1456862575765)
Event(test msg,0.272857979638502,1456862576765)
Event(test msg,0.272857979638502,1456862577764)
Event(test msg,0.272857979638502,1456862578768)
Event(test msg,0.272857979638502,1456862579766)
```

As expected, messages are sent to our server via websocket from the open page, published to Kafka by our server, consumed from Kafka by our listener, and logged to the console at a steady rate. If you open more pages by opening localhost:9000 in multiple browser windows, you should see the rate at which messages are logged increase.

Testing the Server
------------------

But how will our server perform under load? We'd like to avoid opening hundreds of browser tabs to run a test, so we'll create a small load tester application that uses Akka Http to create websocket client connections identical to those created by our test page.

```scala
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
```

This load testing app creates some configurable number of websocket client connections, sends messages through them at a configurable rate, and watches kafka to confirm that the number of messages read from Kafka is the same as the number of messages sent via websocket.

Run the streaming upload server again, close any browser tabs holding the test page, and run this app by using `sbt run` and choosing the first option, which should look something like '[2] com.pkinsky.LoadTest'.

Todo
----

. `KafkaService`: handle Json parsing failures (corrupted messages or messages with missing fields) gracefully
. Dockerize all the things
. Don't assume even small messages are strict: https://github.com/akka/akka/issues/20096
