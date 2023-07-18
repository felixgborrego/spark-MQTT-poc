Data Ops tools
--------------

This package contains additional tools to work and explore the data,
This is not meant to be production ready code, but rather a set of tools to help you on your day today work.

## Tools

* _GenerateFakeEvents_: A tool to generate live fake driver events and publish them to a MQTT

```
sbt "runMain pipelines.tools.GenerateLiveFakeEvents --serverUri=tcp://localhost:1883 --tickPeriod=5"
```

* _LiveTailMqtt_: A tool to tail a MQTT topic and print the messages to the console.

```
sbt "runMain pipelines.tools.LiveTailMqtt \
    --serverUri=tcp://localhost:1883              \
    --topic=output/driver_stats                   \
    --streaming=true"
```

* _GenerateFakeEventsHistory_: A tool to generate fake events history

```
sbt "runMain pipelines.tools.GenerateFakeEventsHistory"
```

* _GenerateFakeEventsHistory_: A tool rebuild the stats history

```
sbt "runMain pipelines.tools.RebuildStatsHistory"
```




