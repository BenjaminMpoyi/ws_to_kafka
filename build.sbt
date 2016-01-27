name          := "ws_to_kafka"

version       := "0.0.1"

scalaVersion  := "2.11.6"

scalacOptions := Seq("-unchecked", "-feature", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaStreamV      = "2.0.1"
  Seq(
    "com.typesafe.akka" %% "akka-stream-experimental"             % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-core-experimental"          % akkaStreamV,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental"    % akkaStreamV,
    "com.softwaremill.reactivekafka" %% "reactive-kafka-core"     % "0.9.0",
    "com.typesafe.play" %% "play-json"                            % "2.4.6"
  )
}

resolvers ++= Seq(
  "Confluentic repository" at "http://packages.confluent.io/maven/"
)
