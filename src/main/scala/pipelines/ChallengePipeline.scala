package pipelines

import poc.input.driver_rated.DriverRated
import poc.input.driver_tipped.DriverTipped
import poc.input.journey_finished.JourneyFinished
import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.sdk.transforms.windowing.{
  AfterProcessingTime,
  AfterWatermark,
  IntervalWindow,
  Repeatedly,
  TimestampCombiner,
  Window
}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import pipelines.models._
import pipelines.utils.Implicits._
import pipelines.utils.Logging

import scala.util.Try

object ChallengePipeline {

  val maxAllowedTimestampSkew = Duration.standardSeconds(20)

  val logger = Logging.getLogger(getClass)

  // Run the pipeline from raw byte input
  // - Decode the protobuf events
  // - Filter out invalid events
  // - Run the pipeline
  def run(
           sc: ScioContext,
           intervalDuration: Duration,
           driverRatedRawInput: SCollection[Array[Byte]],
           driverTippedRawInput: SCollection[Array[Byte]],
           journeyFinishedRawInput: SCollection[Array[Byte]]
         ): SCollection[Array[Byte]] = {
    val driverRatedIn: SCollection[DriverRatedEvent] = driverRatedRawInput
      .withName("Parse DriverRated protobuf")
      .flatMap(bytes => Try(DriverRated.parseFrom(bytes)).toOption)
      .withName("Filter invalid DriverRated")
      .filter(validateDriverRated)
      .map(_.toEvent)

    val driverTippedIn: SCollection[DriverTippedEvent] = driverTippedRawInput
      .withName("Parse DriverTipped protobuf")
      .flatMap(bytes => Try(DriverTipped.parseFrom(bytes)).toOption)
      .withName("Filter invalid DriverTipped protobuf")
      .map(_.toEvent)

    val journeyFinishedIn: SCollection[JourneyFinishedEvent] = journeyFinishedRawInput
      .withName("Parse JourneyFinished")
      .flatMap(bytes => Try(JourneyFinished.parseFrom(bytes)).toOption)
      .withName("Filter invalid JourneyFinished")
      .filter(validateJourneyFinished)
      .map(_.toEvent)

    runPipeline(
      sc,
      intervalDuration,
      driverRatedIn,
      driverTippedIn,
      journeyFinishedIn
    ).flatMap(_.map(_.toProtobuf))
  }

  // Run the pipeline from the decoded input and verified events
  def runPipeline(
                   sc: ScioContext,
                   intervalDuration: Duration,
                   driverRatedIn: SCollection[DriverRatedEvent],
                   driverTippedIn: SCollection[DriverTippedEvent],
                   journeyFinishedIn: SCollection[JourneyFinishedEvent]
                 ): SCollection[Seq[DriverStatsEvent]] = {
    val mergedStatsInput: SCollection[EventStats] = sc
      .unionAll(
        Seq(
          driverRatedIn.map(_.toStats),
          driverTippedIn.map(_.toStats),
          journeyFinishedIn.map(_.toStats)
        )
      )
      .withTimestamp
      .filter { case (e, sourceTimestamp) =>
        // drop messages that are too ald for the current windows
        val tmp =
          !e.timestamp.isBefore(sourceTimestamp.minus(maxAllowedTimestampSkew)) // Filter out events that are too lat
        if (!tmp) {
          logger.warn(
            s"WARNING! dropped message for been too old. Event timestamp: ${e.timestamp} < ${sourceTimestamp} -Skew"
          )
        }
        tmp
      }
      .map(_._1)

    val windowedInputStream: SCollection[(IntervalWindow, Iterable[EventStats])] = mergedStatsInput
      .timestampBy(_.timestamp, allowedTimestampSkew = maxAllowedTimestampSkew)
      .withFixedWindows(
        duration = intervalDuration,
        offset = Duration.ZERO,
        options = customWindowOptions(intervalDuration)
      )
      .withWindow[IntervalWindow]
      .applyTransform(Logging.watermarkLoggingIntervalFn)
      .swap
      .groupByKey // TODO rewrite this as a combine transformation to avoid potential out-of-memory

    windowedInputStream.map { case (window, events) =>
      val statsPerWindow = events
        .groupBy(_.driverId)
        .map { case (driverId, events) =>
          val stats = aggregateStatsPerDriver(driverId, events)
          stats.toEvent(window)
        }
        .toSeq
      (window, statsPerWindow)
    }.tap { case (window, stats) =>
      if (logger.isDebugEnabled) {
        logger.debug(s"--->>>Windows Closed: $window ${stats.mkString("\n")}\n\n")
      }
    }.map(_._2)

  }

  // Compute stats for a single driver for all the events in the window
  // WARNING: This assume the amount of events per driver per window is small enough to fit in memory!
  // TODO: to avoid potential out-of-memory we could use a combine transformation
  private def aggregateStatsPerDriver(driverId: String, values: Iterable[EventStats]): WindowAccStats =
    values.foldLeft(
      new WindowAccStats(driverId, 0, 0, 0, 0, 0, 0, 0)
    )((acc, event) => acc.applyEventStats(event))

  // Define the closing window behaviour
  // - The closing window is triggered one the window interval has passed (counting from the timestamp of the first element in the window)
  def customWindowOptions(intervalDuration: Duration) =
    WindowOptions( //  //
      trigger = Repeatedly.forever(
        AfterProcessingTime
          .pastFirstElementInPane()
          .plusDelayOf(intervalDuration)
      ),
      // TODO Review using the default watermark trigger
      //  This could be a good option, but run into a bug/unexpected issue with the DirectRunner and MqttIO processing time
      // trigger = Repeatedly.forever(AfterWatermark.pastEndOfWindow()),

      allowedLateness = Duration.ZERO, // No late messages allowed
      closingBehavior = Window.ClosingBehavior.FIRE_IF_NON_EMPTY,
      accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
      timestampCombiner = TimestampCombiner.END_OF_WINDOW // output event timestamp is the end of the window
    )

  // Verify DriverRated events are valid
  def validateDriverRated(driverRated: DriverRated): Boolean =
    !driverRated.ratedAt.isEmpty && !driverRated.driverId.isEmpty && !driverRated.journeyId.isEmpty

  // Verify DriverTipped events are valid
  def validateDriverTipped(driverTipped: DriverTipped): Boolean =
    !driverTipped.tippedAt.isEmpty && !driverTipped.driverId.isEmpty && !driverTipped.journeyId.isEmpty

  // Verify a JourneyFinished event is valid
  def validateJourneyFinished(journeyFinished: JourneyFinished): Boolean =
    !journeyFinished.startedAt.isEmpty && !journeyFinished.journeyId.isEmpty && !journeyFinished.endedAt.isEmpty

}
