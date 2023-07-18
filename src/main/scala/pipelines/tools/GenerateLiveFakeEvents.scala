package pipelines.tools

import com.spotify.scio.ScioContext
import com.spotify.scio.extra.json._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions}
import org.apache.beam.sdk.options.Validation.Required
import org.joda.time.{Duration, Instant}
import pipelines.models._
import pipelines.utils.{IO, Logging}
import pipelines.utils.Implicits._

import java.lang
import scala.util.Random

trait GenerateLiveFakeEventsOptions extends PipelineOptions {
  @Description("the MQTT broker host to source the events from")
  @Default.String("tcp://localhost:1883")
  @Required
  def getServerUri: String
  def setServerUri(value: String): Unit

  @Description("MQTT tick period in seconds (interval between events)")
  @Default.Integer(5)
  @Required
  def getTickPeriod: Int
  def setTickPeriod(value: Int): Unit

}

// Utils to generate fake History data for the pipeline as a json
// run locally with sbt "runMain pipelines.tools.GenerateLiveFakeEvents --serverUri=tcp://localhost:1883 --tickPeriod=5"
object GenerateLiveFakeEvents {

  val logger = Logging.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    val (opts, _) = ScioContext.parseArguments[GenerateLiveFakeEventsOptions](args)
    val tickPeriod = Duration.standardSeconds(opts.getTickPeriod)
    val serverUri = opts.getServerUri

    val sc = ScioContext(opts)
    logger.info(s"Generating fake events every $tickPeriod seconds...")

    val clockStream: SCollection[lang.Long] = sc.customInput(
      "clock",
      GenerateSequence
        .from(0)
        .withRate(1, tickPeriod)
    )

    clockStream
      .map(_ => fakeDriverRated)
      .tap(e => logger.info(s"Driver rated $e sent to MQTT"))
      .map(_.toProtobuf)
      .saveAsCustomOutput(
        "driver-rated-out",
        IO.rawSinkToMqtt(
          serverUri = serverUri,
          topic = "input/driver_rated"
        )
      )

    clockStream
      .map(_ => fakeDriverTipped)
      .tap(e => logger.info(s"Driver tipped $e sent to MQTT"))
      .map(_.toProtobuf)
      .saveAsCustomOutput(
        "driver-tipped-out",
        IO.rawSinkToMqtt(
          serverUri = serverUri,
          topic = "input/driver_tipped"
        )
      )

    clockStream
      .map(_ => fakeJourneyFinished)
      .tap(e => logger.info(s"Journey finished $e sent to MQTT"))
      .map(_.toProtobuf)
      .saveAsCustomOutput(
        "journey-finished-out",
        IO.rawSinkToMqtt(
          serverUri = serverUri,
          topic = "input/journey_finished"
        )
      )

    sc.run().waitUntilDone()
  }

  def fakeDriverTipped =
    DriverTippedEvent(
      driverId = s"driver-${Random.nextInt(4)}",
      riderId = s"rider-1-${Random.nextInt(10)}",
      journeyId = s"journey-${Random.nextInt(10)}",
      amount = 5,
      tippedAt = Instant.now()
    )

  def fakeJourneyFinished =
    JourneyFinishedEvent(
      driverId = s"driver-${Random.nextInt(4)}",
      journeyId = s"journey-${Random.nextInt(10)}",
      riderId = s"rider-1-${Random.nextInt(10)}",
      finishReason = "DROP_OFF",
      startedAt = Instant.now().minus(Duration.standardMinutes(30)),
      endedAt = Instant.now(),
      distance = 5,
      price = 10
    )

  def fakeDriverRated =
    DriverRatedEvent(
      driverId = s"driver-${Random.nextInt(4)}",
      riderId = s"rider-1-${Random.nextInt(10)}",
      journeyId = s"journey-${Random.nextInt(10)}",
      rating = 5,
      ratedAt = Instant.now().minus(Duration.standardSeconds(10))
    )
}
