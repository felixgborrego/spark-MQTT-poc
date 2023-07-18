package pipelines.tools

import com.spotify.scio.ScioContext
import com.spotify.scio.extra.json.{JsonScioContext, _}
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.{AfterWatermark, Repeatedly, Window}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import pipelines.ChallengePipeline
import pipelines.utils.Implicits._

import pipelines.models.{DriverRatedEvent, DriverTippedEvent, JourneyFinishedEvent}

// Utils to generate the stats from the history instead of the live stream
// Useful to reprocess historical data, testing and debugging
// Note: it uses the same ChallengePipeline but with the data as bounded input
// run locally with: sbt "runMain pipelines.tools.RebuildStatsHistory"
object RebuildStatsHistory {
  val intervalDuration = Duration.standardMinutes(10)

  def main(args: Array[String]): Unit = {
    val sc = ScioContext()

    val driverRatedEvents = sc.jsonFile[DriverRatedEvent]("fake-data-lake/driver-rated/**")
    val driverTippedEvents = sc.jsonFile[DriverTippedEvent]("fake-data-lake/driver-tipped/*")
    val journeyFinishedEvents = sc.jsonFile[JourneyFinishedEvent]("fake-data-lake/journey-finished/*")

    val stats = ChallengePipeline
      .runPipeline(
        sc,
        intervalDuration,
        driverRatedEvents,
        driverTippedEvents,
        journeyFinishedEvents
      )
      .flatten

    stats.saveAsJsonFile("fake-data-lake/stats")

    sc.run().waitUntilFinish()
  }

  def customWindowOptions() =
    WindowOptions(
      trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
      allowedLateness = Duration.ZERO, // No late messages allowed
      closingBehavior = Window.ClosingBehavior.FIRE_IF_NON_EMPTY,
      accumulationMode = AccumulationMode.ACCUMULATING_FIRED_PANES
    )
}
