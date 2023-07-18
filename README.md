POC using Spark and MQTT to process driver events in real-time
==============================================================

This small POC demonstrates how to process in real-time events from a MQTT topic and calculate driver stats.
For more information about the challenge, please check the [challenge description](CHALLENGE_README.md)

Components:

* The ChallengePipeline is the main pipeline that reads from a MQTT topic and calculates the driver stats.
* Tools: A set of data ops tools to help you on your day today work.

#### Some considerations:

* Implemented using [Scio](https://spotify.github.io/scio/) as it offer a simple abstraction over the Apache Beam Java
  API.

* To support our use-case, we need to aggregate driver stats over a fixed windows period (10 mins by default),
  I'm assuming all the driver evens for the window can fit in memory.

* The MQTT source can not provider the timestamp based on event time, so we are force to use source processing time.
  Workaround: The pipeline silently drop old messages, generating some missing data that needs to be corrected later
  from the historical source.

* The included source use the DirectRunner, but it'll work with minimal changes with the DataflowRunner.

* Missing improvements, there are many important improvements to make this pipeline production ready that
  I didn't include in the solution:
    * Integration test end to end, for instance
      using [testcontainers-scala](https://github.com/testcontainers/testcontainers-scala)
    * Telemetry, monitoring, alerting and metrics
    * CI/CD testing th new pipeline with partial production data

* Codecs: I'm using the default Kyro coder without config to infer Coders at compile time,
  using Magnolia will be a better option.

* Deduplication: There is no deduplication logic in the pipeline, and MqttIO only support at least once delivery
  semantics. So a deduplication logic needs to be implemented

## Build

```
sbt test
sbt package
```

## Run locally

```
# Start the MQTT broker
docker-compose up -d

# Start the Challenge pipeline
sbt "run --serverUri=tcp://localhost:1883 --aggregationInterval=10 --streaming=true"

# Optional - Start the LiveTailMqtt tool to see the output
sbt "runMain pipelines.tools.LiveTailMqtt --serverUri=tcp://localhost:1883 --topic=output/driver_stats --streaming=true"

# Start the GenerateFakeEvents tool to generate fake events
sbt "runMain pipelines.tools.GenerateLiveFakeEvents --serverUri=tcp://localhost:1883 --tickPeriod=5"
```