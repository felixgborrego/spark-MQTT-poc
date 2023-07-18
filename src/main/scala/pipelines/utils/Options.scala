package pipelines.utils

import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, StreamingOptions}

// Pipeline CLI options
// Pipeline configuration for the MQTT Broker and topics
trait MQTTBrokerOption extends PipelineOptions {
  @Description("the MQTT broker host to source the events from")
  @Required
  def getServerUri: String

  def setServerUri(value: String): Unit

  @Description("MQTT topic to source the DriverRated even")
  @Default.String("input/driver_rated")
  def getDriverRatedTopic: String

  def setDriverRatedTopic(value: String): Unit

  @Description("MQTT topic to source the DriverTipped event")
  @Default.String("input/driver_tipped")
  def getDriverTippedTopic: String

  def setDriverTippedTopic(value: String): Unit

  @Description("MQTT topic to source the JourneyFinished event")
  @Default.String("input/journey_finished")
  def getJourneyFinishedTopic: String

  def setJourneyFinishedTopic(value: String): Unit

  @Description("MQTT topic to sink the DriverStats event")
  @Default.String("output/driver_stats")
  def getSinkDriverStatsTopic: String

  def setSinkDriverStatsTopic(value: String): Unit
}

// Pipeline option definition
trait pocChallengeOptions // poc challenge specific options
  extends PipelineOptions // Generic options for all pipelines
    with StreamingOptions
    // with DataflowPipelineOptions // GCP specifics (project, region, runner, v2 autoscaling, etc) not included
    // with DirectOptions
    with MQTTBrokerOption {

  @Description("Aggregation interval in seconds")
  @Default.Integer(60 * 10)
  @Required
  def getAggregationInterval: Int

  def setAggregationInterval(value: Int): Unit
}
