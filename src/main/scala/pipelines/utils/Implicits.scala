package pipelines.utils

import poc.input.driver_rated.DriverRated
import poc.input.driver_tipped.DriverTipped
import poc.input.journey_finished.{FinishReason, JourneyFinished}
import poc.output.driver_stats.DriverStats
import com.google.protobuf.timestamp.Timestamp
import io.circe.{Decoder, Encoder}
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow}
import org.joda.time.Instant
import pipelines.models._

// Boilerplate to convert between between types
object Implicits {

  // Convert protobuf timestamp to Joda Instant
  implicit class TimestampOps(timestamp: Timestamp) {
    def toInstant: Instant =
      new Instant(timestamp.seconds * 1000 + timestamp.nanos / 1000000)
  }

  implicit class DriverRatedOps(messge: DriverRated) {
    def toEvent = DriverRatedEvent(
      driverId = messge.driverId,
      riderId = messge.riderId,
      journeyId = messge.journeyId,
      rating = messge.rating,
      ratedAt = messge.ratedAt.getOrElse(sys.error("rateAt is required")).toInstant
    )
  }

  implicit class DriverTippedOps(messge: DriverTipped) {
    def toEvent = DriverTippedEvent(
      driverId = messge.driverId,
      riderId = messge.riderId,
      journeyId = messge.journeyId,
      amount = messge.amount,
      tippedAt = messge.tippedAt.getOrElse(sys.error("tippedAt is required")).toInstant
    )
  }

  implicit class JourneyFinishedOps(messge: JourneyFinished) {
    def toEvent = JourneyFinishedEvent(
      driverId = messge.driverId,
      riderId = messge.riderId,
      journeyId = messge.journeyId,
      startedAt = messge.startedAt.getOrElse(sys.error("startedAt is required")).toInstant,
      endedAt = messge.endedAt.getOrElse(sys.error("endedAt is required")).toInstant,
      finishReason = messge.finishReason.toString(),
      distance = messge.distance,
      price = messge.price
    )
  }

  implicit class InstantOps(instant: Instant) {
    // Convert Joda Instant to com.google.protobuf.Timestamp
    def toTimestamp: Timestamp =
      Timestamp(instant.getMillis / 1000, (instant.getMillis % 1000 * 1000000).toInt)
  }

  implicit class WindowAccStatsOps(acc: WindowAccStats) {
    // Convert internal accumulator to the protobuf DriverStats message
    def toEvent(window: IntervalWindow): DriverStatsEvent =
      DriverStatsEvent(
        driverId = acc.driverId,
        intervalStart = window.start(),
        intervalEnd = window.end(),
        totalDropOffs = acc.totalDropOffs,
        totalDriverCancels = acc.totalDriverCancels,
        totalDistance = acc.totalDistance,
        totalPrice = acc.totalPrice,
        totalTips = acc.totalTips,
        avgRating = acc.avgRating
      )

  }

  implicit class EventOps[E <: Event](event: E) {

    // Encode the event to an protobuf message
    def toProtobuf = event match {
      case e: DriverRatedEvent =>
        DriverRated(
          driverId = e.driverId,
          riderId = e.riderId,
          journeyId = e.journeyId,
          rating = e.rating,
          ratedAt = Some(e.ratedAt.toTimestamp)
        ).toByteArray
      case e: DriverTippedEvent =>
        DriverTipped(
          driverId = e.driverId,
          riderId = e.riderId,
          journeyId = e.journeyId,
          amount = e.amount,
          tippedAt = Some(e.tippedAt.toTimestamp)
        ).toByteArray
      case e: JourneyFinishedEvent =>
        JourneyFinished(
          driverId = e.driverId,
          riderId = e.riderId,
          journeyId = e.journeyId,
          finishReason = e.finishReason match {
            case "DROP_OFF" => FinishReason.DROP_OFF
            case "DRIVER_CANCEL" => FinishReason.DRIVER_CANCEL
            case "RIDER_CANCEL" => FinishReason.RIDER_CANCEL
            case "ADMIN_CANCEL" => FinishReason.ADMIN_CANCEL
            case other => sys.error(s"Unsupported finish reason $other")
          },
          startedAt = Some(e.startedAt.toTimestamp),
          endedAt = Some(e.endedAt.toTimestamp),
          distance = e.distance,
          price = e.price
        ).toByteArray
      case e: DriverStatsEvent =>
        DriverStats(
          driverId = e.driverId,
          intervalStart = Some(e.intervalStart.toTimestamp),
          intervalEnd = Some(e.intervalEnd.toTimestamp),
          totalDropOffs = e.totalDropOffs,
          totalDriverCancels = e.totalDriverCancels,
          totalDistance = e.totalDistance,
          totalPrice = e.totalPrice,
          totalTips = e.totalTips,
          avgRating = e.avgRating
        ).toByteArray
      case other => sys.error(s"Protobuf event not supported, Cannot encode as protobuf from $other")
    }
  }

  // Define a custom Circe encoder/decoder for Instant
  implicit val instantEncoder: Encoder[Instant] = Encoder.encodeString.contramap[Instant](_.toString)
  implicit val instantDecoder: Decoder[Instant] = Decoder.decodeString.emapTry { str =>
    scala.util.Try(Instant.parse(str))
  }

  //  implicit val JourneyFinishReasonEncoder: Encoder[JourneyFinishReason] =
  //    Encoder.encodeString.contramap[JourneyFinishReason](_.toString)
  //
  //  implicit val JourneyFinishReasonDecoder: Decoder[JourneyFinishReason] = Decoder.decodeString.emapTry { str =>
  //    scala.util.Try(JourneyFinishReason.fromString(str))
  //  }

}
