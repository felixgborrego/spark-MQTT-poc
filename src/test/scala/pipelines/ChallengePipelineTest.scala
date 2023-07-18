package pipelines

import com.spotify.scio.testing._
import org.joda.time.{Duration, Instant}
import pipelines.models._
import com.spotify.scio.testing._
import org.apache.beam.sdk.transforms.windowing.IntervalWindow

class ChallengePipelineTest extends PipelineSpec {

  val now = Instant.parse("2020-01-01T00:00:00.000Z")
  val baseTime = now
  val intervalDuration = Duration.standardSeconds(10)

  val intervalWindow1 = new IntervalWindow(baseTime, intervalDuration)
  val intervalWindow2 = new IntervalWindow(intervalWindow1.end(), intervalDuration)
  val intervalWindow3 = new IntervalWindow(intervalWindow2.end(), intervalDuration)

  val driverRatedStream = testStreamOf[DriverRatedEvent]
    // Start at the epoch
    .advanceWatermarkTo(now.minus(Duration.standardSeconds(1)))
    // add some elements ahead of the watermark
    .addElements(
      DriverRatedEvent("driver-1", "b", "c", 2, now),
      DriverRatedEvent("driver-1", "b", "c", 4, now.plus(Duration.standardSeconds(1))),
      DriverRatedEvent("driver-2", "b", "c", 5, now.plus(Duration.standardSeconds(2)))
    )
    .advanceWatermarkToInfinity()

  val driverTippedStream = testStreamOf[DriverTippedEvent]
    .advanceWatermarkTo(now.minus(Duration.standardSeconds(1)))
    .addElements(
      DriverTippedEvent("driver-1", "b", "c", 2, now),
      DriverTippedEvent("driver-1", "b", "c", 4, now.plus(Duration.standardSeconds(1)))
    )
    .advanceWatermarkToInfinity()

  val journeyFinishedStream = testStreamOf[JourneyFinishedEvent]
    .advanceWatermarkTo(now.minus(Duration.standardSeconds(1)))
    .addElements(
      JourneyFinishedEvent("driver-1", "b", "c", "DROP_OFF", now.minus(Duration.standardMinutes(1)), now, 1, 10),
      JourneyFinishedEvent(
        "driver-1",
        "b",
        "c",
        "DROP_OFF",
        now.minus(Duration.standardMinutes(1)),
        now.plus(Duration.standardSeconds(2)),
        1,
        10
      ),
      JourneyFinishedEvent(
        "driver-1",
        "b",
        "c",
        "DRIVER_CANCEL",
        now.minus(Duration.standardMinutes(1)),
        now.plus(Duration.standardSeconds(2)),
        1,
        10
      ),
      JourneyFinishedEvent(
        "driver-1",
        "b",
        "c",
        "DROP_OFF",
        now.minus(Duration.standardMinutes(1)),
        now.plus(Duration.standardSeconds(2)).plus(intervalDuration), // to make it arrive in the window2
        1,
        10
      )
    )
    .advanceWatermarkToInfinity()

  val expectedWindow1 = Seq(
    DriverStatsEvent("driver-1", intervalWindow1.start(), intervalWindow1.end(), 2, 1, 3, 30, 6, (4 + 2) / 2f),
    DriverStatsEvent("driver-2", intervalWindow1.start(), intervalWindow1.end(), 0, 0, 0, 0, 0, 5)
  )
  val expectedWindow2 = Seq(
    DriverStatsEvent("driver-1", intervalWindow2.start(), intervalWindow2.end(), 1, 0, 1, 10, 0, 0)
  )

  "ChallengePipeline" should "generate the expected window stats" in {

    runWithContext { sc =>
      val result = ChallengePipeline
        .runPipeline(
          sc,
          intervalDuration,
          sc.testStream(driverRatedStream),
          sc.testStream(driverTippedStream),
          sc.testStream(journeyFinishedStream)
        )
        .flatten

      result should inOnTimePane(intervalWindow1) {
        containInAnyOrder(expectedWindow1)
      }
      result should inOnTimePane(intervalWindow2) {
        containInAnyOrder(expectedWindow2)
      }
      result should inOnTimePane(intervalWindow3) {
        beEmpty
      }
      result should haveSize(3)
      result should containInAnyOrder(expectedWindow1 ++ expectedWindow2)
    }
  }

}
