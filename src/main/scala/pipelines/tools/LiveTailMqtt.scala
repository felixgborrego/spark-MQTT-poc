package pipelines.tools

import poc.input.driver_rated.DriverRated
import poc.input.driver_tipped.DriverTipped
import poc.input.journey_finished.JourneyFinished
import poc.output.driver_stats.DriverStats
import com.spotify.scio.ScioContext
import org.apache.beam.sdk.options.{Description, PipelineOptions}
import org.apache.beam.sdk.options.Validation.Required
import pipelines.utils.Implicits._
import pipelines.utils.{IO, Logging}

import scala.util.Try

trait LiveTailMqttOptions extends PipelineOptions {
  @Description("the MQTT broker host to source the events from")
  @Required
  def getServerUri: String

  def setServerUri(value: String): Unit

  @Description("MQTT topic to tail")
  @Required
  def getTopic: String

  def setTopic(value: String): Unit
}

// A utility to tail the MQTT pipeline
// run locally with sbt "runMain pipelines.tools.LiveTailMqtt --serverUri=tcp://localhost:1883 --topic=output/driver_stats --streaming=true"
object LiveTailMqtt {
  val logger = Logging.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val (opts, _) = ScioContext.parseArguments[LiveTailMqttOptions](args)
    val sc = ScioContext(opts)

    IO.rawSourceFromMqtt(
      serverUri = opts.getServerUri,
      topic = opts.getTopic,
      sc
    ).flatMap { bytes =>
      // Naive way to parse the bytes into one of our messages
      Try(DriverTipped.parseFrom(bytes).toEvent.toString)
        .orElse(Try(DriverRated.parseFrom(bytes).toEvent.toString))
        .orElse(Try(JourneyFinished.parseFrom(bytes).toEvent.toString))
        .orElse(Try(DriverStats.parseFrom(bytes).toString))
        .toOption
    }.debug(prefix = "📥 Message: ")

    logger.info(s"🚀 Tailing ...")
    sc.run().waitUntilFinish()
  }
}
