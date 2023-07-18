package pipelines.tools

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.json._
import io.circe.Encoder
import org.joda.time.{Duration, Instant}
import pipelines.models._
import pipelines.utils.Implicits._

import scala.util.Random

// Simple tool to generate fake History data for the pipeline as a json
// run locally with: sbt "runMain pipelines.tools.GenerateFakeEventsHistory"
object GenerateFakeEventsHistory {

  def main(args: Array[String]): Unit = {
    val sc = ScioContext()

    sc
      .parallelize(1 to 100)
      .map(fakeDriverRated)
      .saveAsJsonFile("fake-data-lake/driver-rated")

    sc.parallelize(1 to 100)
      .map(fakeDriverTipped)
      .saveAsJsonFile("fake-data-lake/driver-tipped")

    sc.parallelize(1 to 100)
      .map(fakeJourneyFinished)
      .saveAsJsonFile("fake-data-lake/journey-finished")

    sc.run().waitUntilDone()
  }

  def fakeDriverTipped(secs: Int) =
    DriverTippedEvent(
      driverId = s"driver-${Random.nextInt(10)}",
      riderId = s"rider-1-${Random.nextInt(10)}",
      journeyId = s"journey-${Random.nextInt(10)}",
      amount = 5,
      tippedAt = Instant.now().plus(Duration.standardSeconds(secs))
    )

  def fakeJourneyFinished(secs: Int) =
    JourneyFinishedEvent(
      journeyId = s"journey-${Random.nextInt(10)}",
      driverId = s"driver-${Random.nextInt(10)}",
      riderId = s"rider-1-${Random.nextInt(10)}",
      finishReason = "DROP_OFF",
      startedAt = Instant.now().minus(Duration.standardMinutes(30)).plus(Duration.standardSeconds(secs)),
      endedAt = Instant.now().plus(Duration.standardSeconds(secs)),
      distance = 5,
      price = 10
    )

  def fakeDriverRated(secs: Int) =
    DriverRatedEvent(
      driverId = s"driver-${Random.nextInt(10)}",
      riderId = s"rider-1-${Random.nextInt(10)}",
      journeyId = s"journey-${Random.nextInt(10)}",
      rating = 5,
      ratedAt = Instant.now().plus(Duration.standardSeconds(secs))
    )

}
