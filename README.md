# Server that consumes events via many websocket connections and publishes them to a single Kafka topic

== what is akka streams (quick, with links for more info)


== first, we want to publish messages to kafka.
//(build thing)

== now that we have kafka pub svc, we need to accept incoming websocket connections
//build thing, single route

== now that we have server that takes msgs and pubs to kafka, let's test it
//build single-ws client thing
//subscribe to kafka, confirm that message shows up

== let's stress test it a bit

//1024 connections, 1024 msgs

== cool, no?